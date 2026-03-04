from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Optional, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.deps import get_db, get_current_user
from app.models.user import User

router = APIRouter(prefix="/api/admin/staff", tags=["admin_staff"])


# ----------------------
# Schemas (프론트 기대 형태)
# ----------------------

class PerformanceSummary(BaseModel):
    company_project_count: int = 0
    company_project_score_sum: float = 0.0
    employee_project_count: int = 0
    employee_project_score_sum: float = 0.0
    employee_allocated_score_sum: float = 0.0
    employee_share_percent: float = 0.0  # 0~100


class AttendanceSummary(BaseModel):
    total_days: int = 0
    actual_work_days: int = 0
    total_work_hours: float = 0.0
    avg_work_hours: float = 0.0
    office_days: int = 0
    offsite_days: int = 0
    annual_leave_days: int = 0
    half_leave_days: int = 0
    overtime_days: int = 0
    holiday_work_days: int = 0
    extra_work_days: int = 0


class ProjectRow(BaseModel):
    project_id: int
    project_name: str
    evaluated_at: str
    project_final_score: float
    personal_score: float
    allocated_score: float


class StaffReportOut(BaseModel):
    unit: Literal["month", "year"]
    date: str
    user_id: int
    employee_name: str
    department_name: Optional[str] = None
    performance: PerformanceSummary
    attendance: AttendanceSummary
    projects: List[ProjectRow]


def _require_admin(user: Optional[User]) -> User:
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    # 대표님 환경: role_id == 6 이 관리자
    if int(getattr(user, "role_id", 0) or 0) != 6:
        raise HTTPException(status_code=403, detail="관리자 권한이 필요합니다.")
    return user


def _parse_range(unit: str, date_str: str) -> tuple[dt.datetime, dt.datetime]:
    """
    unit=month: date=YYYY-MM
    unit=year : date=YYYY
    반환: [start, end) (KST 기준으로 단순 계산; DB는 TIMESTAMPTZ라도 범위 필터는 안전)
    """
    if unit == "month":
        try:
            y, m = map(int, date_str.split("-"))
            start = dt.datetime(y, m, 1)
            # 다음달 1일
            if m == 12:
                end = dt.datetime(y + 1, 1, 1)
            else:
                end = dt.datetime(y, m + 1, 1)
            return start, end
        except Exception:
            raise HTTPException(status_code=400, detail="date 형식이 올바르지 않습니다. (YYYY-MM)")
    if unit == "year":
        try:
            y = int(date_str)
            start = dt.datetime(y, 1, 1)
            end = dt.datetime(y + 1, 1, 1)
            return start, end
        except Exception:
            raise HTTPException(status_code=400, detail="date 형식이 올바르지 않습니다. (YYYY)")
    raise HTTPException(status_code=400, detail="unit 값이 올바르지 않습니다. (month|year)")


@router.get("/report", response_model=StaffReportOut)
def get_staff_report(
    unit: Literal["month", "year"] = Query(..., description="month|year"),
    date: str = Query(..., description="YYYY-MM 또는 YYYY"),
    user_id: int = Query(..., ge=1),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> StaffReportOut:
    """
    운영관리 > 직원관리 리포트
    - '현재 완료 프로젝트만' 집계: project_completion_snapshots.is_active = true 필터 적용
    - 완료 범위: completed_at 기준(unit/date 범위)
    """
    _require_admin(current_user)

    start, end = _parse_range(unit, date)

    # 직원 기본 정보
    u = db.execute(
        text(
            """
            SELECT u.id, u.name, d.name AS department_name
            FROM users u
            LEFT JOIN departments d ON d.id = u.department_id
            WHERE u.id = :uid
            """
        ),
        {"uid": user_id},
    ).mappings().first()
    if not u:
        raise HTTPException(status_code=404, detail="직원을 찾을 수 없습니다.")

    # 스냅샷 테이블 존재 확인 (없으면 빈 리포트 반환)
    has_snap = db.execute(text("SELECT to_regclass('public.project_completion_snapshots') IS NOT NULL")).scalar() is True
    has_item = db.execute(text("SELECT to_regclass('public.project_completion_snapshot_items') IS NOT NULL")).scalar() is True
    if not (has_snap and has_item):
        return StaffReportOut(
            unit=unit,
            date=date,
            user_id=int(user_id),
            employee_name=str(u["name"]),
            department_name=u.get("department_name"),
            performance=PerformanceSummary(),
            attendance=AttendanceSummary(),
            projects=[],
        )

    # 회사 전체(현재 완료) 프로젝트
    company = db.execute(
        text(
            """
            SELECT
              COUNT(*)::int AS cnt,
              COALESCE(SUM(pcs.final_project_score), 0)::float AS sum_score
            FROM project_completion_snapshots pcs
            WHERE pcs.is_active = true
              AND pcs.completed_at >= :start AND pcs.completed_at < :end
            """
        ),
        {"start": start, "end": end},
    ).mappings().first() or {"cnt": 0, "sum_score": 0.0}

    # 직원 참여 프로젝트 rows
    rows = db.execute(
        text(
            """
            SELECT
              pcs.project_id AS project_id,
              p.name AS project_name,
              pcs.completed_at AS evaluated_at,
              COALESCE(pcs.final_project_score, 0)::float AS project_final_score,
              COALESCE(i.user_eval_score, 0)::float AS personal_score,
              COALESCE(i.converted_score, 0)::float AS allocated_score
            FROM project_completion_snapshots pcs
            JOIN project_completion_snapshot_items i ON i.snapshot_id = pcs.id
            LEFT JOIN projects p ON p.id = pcs.project_id
            WHERE pcs.is_active = true
              AND pcs.completed_at >= :start AND pcs.completed_at < :end
              AND i.user_id = :uid
            ORDER BY pcs.completed_at DESC, pcs.project_id DESC
            """
        ),
        {"start": start, "end": end, "uid": user_id},
    ).mappings().all()

    employee_project_count = len({int(r["project_id"]) for r in (rows or [])})
    employee_project_score_sum = float(sum(float(r["project_final_score"] or 0.0) for r in (rows or [])))
    employee_allocated_score_sum = float(sum(float(r["allocated_score"] or 0.0) for r in (rows or [])))

    employee_share_percent = 0.0
    if employee_project_score_sum > 0:
        employee_share_percent = (employee_allocated_score_sum / employee_project_score_sum) * 100.0

    projects_out = [
        ProjectRow(
            project_id=int(r["project_id"]),
            project_name=str(r.get("project_name") or f"프로젝트#{int(r['project_id'])}"),
            evaluated_at=str(r.get("evaluated_at")),
            project_final_score=float(r.get("project_final_score") or 0.0),
            personal_score=float(r.get("personal_score") or 0.0),
            allocated_score=float(r.get("allocated_score") or 0.0),
        )
        for r in (rows or [])
    ]

    perf = PerformanceSummary(
        company_project_count=int(company.get("cnt") or 0),
        company_project_score_sum=float(company.get("sum_score") or 0.0),
        employee_project_count=int(employee_project_count),
        employee_project_score_sum=float(employee_project_score_sum),
        employee_allocated_score_sum=float(employee_allocated_score_sum),
        employee_share_percent=float(employee_share_percent),
    )

    # 근태 요약은 기존 근태모듈과 결합 전까지 0으로 반환 (화면 404 해결이 1순위)
    att = AttendanceSummary()

    return StaffReportOut(
        unit=unit,
        date=date,
        user_id=int(user_id),
        employee_name=str(u["name"]),
        department_name=u.get("department_name"),
        performance=perf,
        attendance=att,
        projects=projects_out,
    )
