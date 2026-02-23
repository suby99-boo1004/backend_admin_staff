from __future__ import annotations

"""
Admin Staff Report Router (FIXED)

- GET /api/admin/staff/report?unit=month|year&date=YYYY-MM|YYYY&user_id=1

핵심:
- 프로젝트 성과: project_evaluations(created_at) 기준으로 '평가가 존재하는 프로젝트'를 집계
- 회사 총 사업 수: DISTINCT project_id
- 회사 총 프로젝트 점수 합: SUM(contract_amount) / 10,000  (만원 단위)  ※ 대표님 기준
- 참여 프로젝트 회사 점수 합: "프로젝트 최종 점수(P)" 합
  P = (contract_amount/1,000,000) + project_period_days + difficulty + profit_rate_score + progress_step + participant_count
  profit_rate_score는 projects.profit_rate가 있으면 사용, 없으면 (contract_amount - 비용합)/1,000,000 으로 계산
- 개인 환산 점수: (P/10) * score(0~10)
- 근태: attendance_records 기반(ENUM 안전 캐스팅)

주의:
- DB 컬럼이 버전별로 다를 수 있으므로, projects 컬럼은 존재할 때만 사용(없으면 0 처리)
- 이번 패치는 "500 방지 + 대표님 기준 숫자 일치"를 우선
"""

import datetime as dt
import math
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.deps import get_db, get_current_user
from app.models.user import User

router = APIRouter(prefix="/api/admin/staff", tags=["AdminStaff"])

ROLE_ADMIN_CODE = "ADMIN"


# -----------------------------------------------------------------------------
# Helpers: schema probing
# -----------------------------------------------------------------------------
def _table_exists(db: Session, table: str) -> bool:
    r = db.execute(
        text(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema='public' AND table_name=:t
            LIMIT 1
            """
        ),
        {"t": table},
    ).first()
    return r is not None


def _col_exists(db: Session, table: str, col: str) -> bool:
    r = db.execute(
        text(
            """
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name=:t AND column_name=:c
            LIMIT 1
            """
        ),
        {"t": table, "c": col},
    ).first()
    return r is not None


def _is_admin_db(db: Session, user: User) -> bool:
    role_id = getattr(user, "role_id", None)
    if not role_id:
        return False
    code = db.execute(text("SELECT code FROM roles WHERE id = :id"), {"id": role_id}).scalar()
    return code == ROLE_ADMIN_CODE


def _require_admin(db: Session, user: Optional[User]) -> User:
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    if not _is_admin_db(db, user):
        raise HTTPException(status_code=403, detail="관리자 권한이 필요합니다.")
    return user


def _period_range(unit: str, date_str: str) -> Tuple[dt.datetime, dt.datetime]:
    # 내부는 [start, end) (exclusive end)
    if unit == "month":
        if len(date_str) != 7:
            raise HTTPException(status_code=400, detail="month는 date=YYYY-MM 형식이어야 합니다.")
        y = int(date_str[:4])
        m = int(date_str[5:7])
        start = dt.datetime(y, m, 1, 0, 0, 0)
        if m == 12:
            end = dt.datetime(y + 1, 1, 1, 0, 0, 0)
        else:
            end = dt.datetime(y, m + 1, 1, 0, 0, 0)
        return start, end
    if unit == "year":
        if len(date_str) != 4:
            raise HTTPException(status_code=400, detail="year는 date=YYYY 형식이어야 합니다.")
        y = int(date_str)
        return dt.datetime(y, 1, 1, 0, 0, 0), dt.datetime(y + 1, 1, 1, 0, 0, 0)
    raise HTTPException(status_code=400, detail="unit은 month|year 중 하나여야 합니다.")


# -----------------------------------------------------------------------------
# Schemas
# -----------------------------------------------------------------------------
class PerformanceSummary(BaseModel):
    company_project_count: int = 0
    company_project_score_sum: float = 0.0  # 회사 전체 final_score 합
    employee_project_count: int = 0
    employee_project_score_sum: float = 0.0  # P 합
    employee_allocated_score_sum: float = 0.0
    employee_share_percent: float = 0.0


class AttendanceSummary(BaseModel):
    total_days: int = 0
    actual_work_days: int = 0
    total_work_hours: float = 0.0
    avg_work_hours: float = 0.0

    office_days: int = 0
    offsite_days: int = 0
    annual_leave_days: float = 0.0
    half_leave_days: float = 0.0
    overtime_days: int = 0
    holiday_work_days: int = 0
    extra_work_days: int = 0


class ProjectRow(BaseModel):
    project_id: int
    project_name: str
    evaluated_at: dt.datetime
    project_final_score: float  # 프로젝트 총점(최종 점수)
    personal_score: float       # 개인 평가 점수(0~10)
    allocated_score: float      # 환산 점수
    score_source: str           # 'SNAPSHOT' | 'LIVE'


class StaffReportOut(BaseModel):
    unit: str
    date: str
    user_id: int
    employee_name: str
    department_name: Optional[str] = None
    performance: PerformanceSummary
    attendance: AttendanceSummary
    projects: List[ProjectRow]


# -----------------------------------------------------------------------------
# Core: employee info
# -----------------------------------------------------------------------------
def _load_employee_info(db: Session, user_id: int) -> Tuple[str, Optional[str]]:
    row = db.execute(
        text(
            """
            SELECT u.name AS name, d.name AS department_name
            FROM users u
            LEFT JOIN departments d ON d.id = u.department_id
            WHERE u.id = :uid
            """
        ),
        {"uid": user_id},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")
    return str(row.get("name") or f"#{user_id}"), row.get("department_name")


# -----------------------------------------------------------------------------
# Core: project performance (project_evaluations 기준)
# -----------------------------------------------------------------------------
def _select_expr(db: Session, col: str, default_sql: str = "0") -> str:
    # projects.<col> 이 있으면 COALESCE(p.col,0) 형태로 반환, 없으면 default_sql
    if _col_exists(db, "projects", col):
        return f"COALESCE(p.{col}, 0)"
    return default_sql


def _trunc1(x: float) -> float:
    # 소수 2째자리 이하는 버림(=소수 1자리까지)
    try:
        return math.trunc(float(x) * 10.0) / 10.0
    except Exception:
        return 0.0


def _calc_final_score_row(db: Session, r: Dict[str, Any]) -> float:
    """프로젝트 상세페이지의 '프로젝트 평가 총점수'와 동일 규칙.

    - 영업점수(sales_score)
    - 사업기간(project_period_days)
    - 사업난이도(difficulty)
    - 진행과정(progress_step)
    - 업무진행속도(work_speed)
    - 내부진행점수(internal_score)
    - 외부평가점수(external_score)
    - 참여자수(participant_count)
    - 수익률(금액)(profit_rate)  # 점수로 저장된 값
    합계 후 소수 1자리까지, 2째자리 이하는 버림.
    """
    sales_score = float(r.get("sales_score") or 0.0)
    project_period_days = float(r.get("project_period_days") or 0.0)
    difficulty = float(r.get("difficulty") or 0.0)
    progress_step = float(r.get("progress_step") or 0.0)
    work_speed = float(r.get("work_speed") or 0.0)
    internal_score = float(r.get("internal_score") or 0.0)
    external_score = float(r.get("external_score") or 0.0)
    participant_count = float(r.get("participant_count") or 0.0)
    profit_rate_score = float(r.get("profit_rate") or 0.0)

    total = (
        sales_score
        + project_period_days
        + difficulty
        + progress_step
        + work_speed
        + internal_score
        + external_score
        + participant_count
        + profit_rate_score
    )
    return _trunc1(total)


    project_period_days = float(r.get("project_period_days") or 0.0)
    difficulty = float(r.get("difficulty") or 0.0)
    progress_step = float(r.get("progress_step") or 0.0)
    participant_count = float(r.get("participant_count") or 0.0)

    # profit_rate_score:
    # 1) projects.profit_rate가 있으면(점수로 저장되는 값) 그걸 사용
    pr = r.get("profit_rate")
    if pr is not None:
        profit_rate_score = float(pr or 0.0)
    else:
        # 2) 없으면 (수주 - 비용합) / 1,000,000
        cost_sum = float(r.get("cost_sum") or 0.0)
        profit_rate_score = (contract_amount - cost_sum) / 1_000_000.0

    final_score = project_period_days + difficulty + profit_rate_score + progress_step + participant_count
    # 프론트처럼 소수 1자리 느낌으로(여기서는 1자리까지 반올림)
    return round(final_score, 1)


def _calc_project_performance(db: Session, user_id: int, start_dt: dt.datetime, end_dt: dt.datetime) -> Tuple[PerformanceSummary, List[ProjectRow]]:
    if not _table_exists(db, "project_evaluations"):
        return PerformanceSummary(), []

    # 1) 회사 기준 프로젝트 목록
    # 기본은 '사업 완료' 시점을 기준으로 집계 (가능하면 completed_at 기준)
    if _col_exists(db, "projects", "completed_at"):
        proj_rows = db.execute(
            text(
                """
                SELECT p.id AS project_id
                FROM projects p
                WHERE p.completed_at >= :s AND p.completed_at < :e
                """
            ),
            {"s": start_dt, "e": end_dt},
        ).mappings().all()
    else:
        # fallback: 완료시각 컬럼이 없는 경우, 평가가 생성된 시점 기준(기존 방식)
        proj_rows = db.execute(
            text(
                """
                SELECT DISTINCT pe.project_id
                FROM project_evaluations pe
                WHERE pe.created_at >= :s AND pe.created_at < :e
                """
            ),
            {"s": start_dt, "e": end_dt},
        ).mappings().all()

    proj_ids = [int(r["project_id"]) for r in proj_rows]
    if not proj_ids:
        return PerformanceSummary(), []

    # 2) projects 필요한 컬럼을 "존재할 때만" SELECT 하도록 구성
    # 비용 컬럼: 없으면 0으로 cost_sum 계산
    cost_material = _select_expr(db, "cost_material", "0")
    cost_labor = _select_expr(db, "cost_labor", "0")
    cost_office = _select_expr(db, "cost_office", "0")
    cost_other = _select_expr(db, "cost_other", "0")
    sales_cost = _select_expr(db, "sales_cost", "0")
    cost_progress = _select_expr(db, "cost_progress", "0")

    sales_score = _select_expr(db, "sales_score", "0")
    work_speed = _select_expr(db, "work_speed", "0")
    internal_score = _select_expr(db, "internal_score", "0")
    external_score = _select_expr(db, "external_score", "0")

    project_period_days = _select_expr(db, "project_period_days", "0")
    difficulty = _select_expr(db, "difficulty", "0")
    progress_step = _select_expr(db, "progress_step", "0")
    participant_count = _select_expr(db, "participant_count", "0")

    # profit_rate는 있을 수도/없을 수도
    profit_rate_expr = "p.profit_rate" if _col_exists(db, "projects", "profit_rate") else "NULL"

    select_sql = f"""
        SELECT
          p.id,
          p.name,
          COALESCE(p.contract_amount,0)::float AS contract_amount,
          ({cost_material}+{cost_labor}+{cost_office}+{cost_other}+{sales_cost}+{cost_progress})::float AS cost_sum,
          {sales_score}::float AS sales_score,
          {project_period_days}::float AS project_period_days,
          {difficulty}::float AS difficulty,
          {progress_step}::float AS progress_step,
          {work_speed}::float AS work_speed,
          {internal_score}::float AS internal_score,
          {external_score}::float AS external_score,
          {participant_count}::float AS participant_count,
          {profit_rate_expr} AS profit_rate
        FROM projects p
        WHERE p.id = ANY(:ids)
    """

    # 3) 회사 총 프로젝트 점수 합: final_score 합 (※ contract_amount/1,000,000 항목은 이미 final_score에서 제외됨)
    all_rows = db.execute(text(select_sql), {"ids": proj_ids}).mappings().all()
    all_map = {int(r["id"]): dict(r) for r in all_rows}
    # 3-A) 사업완료 스냅샷(있으면) 우선 사용: 프로젝트 총점(final_project_score) / 완료시각(completed_at)
    snap_map: Dict[int, Dict[str, Any]] = {}
    item_map: Dict[int, Dict[str, float]] = {}
    if _table_exists(db, "project_completion_snapshots"):
        snap_rows = db.execute(
            text(
                """
                SELECT project_id,
                       COALESCE(final_project_score,0)::float AS final_project_score,
                       completed_at
                FROM project_completion_snapshots
                WHERE is_active = true
                  AND project_id = ANY(:ids)
                """
            ),
            {"ids": proj_ids},
        ).mappings().all()

        for sr in snap_rows:
            pid0 = int(sr["project_id"])
            snap_map[pid0] = {
                "final_project_score": float(sr.get("final_project_score") or 0.0),
                "completed_at": sr.get("completed_at"),
            }

        # 직원(본인) 스냅샷 아이템: user_eval_score / converted_score
        if snap_map and _table_exists(db, "project_completion_snapshot_items"):
            item_rows = db.execute(
                text(
                    """
                    SELECT pcs.project_id,
                           COALESCE(psi.user_eval_score,0)::float AS user_eval_score,
                           COALESCE(psi.converted_score,0)::float AS converted_score
                    FROM project_completion_snapshot_items psi
                    JOIN project_completion_snapshots pcs ON pcs.id = psi.snapshot_id
                    WHERE pcs.is_active = true
                      AND pcs.project_id = ANY(:ids)
                      AND psi.user_id = :uid
                    """
                ),
                {"ids": proj_ids, "uid": user_id},
            ).mappings().all()

            for ir in item_rows:
                pid1 = int(ir["project_id"])
                item_map[pid1] = {
                    "user_eval_score": float(ir.get("user_eval_score") or 0.0),
                    "converted_score": float(ir.get("converted_score") or 0.0),
                }

    company_P_sum = 0.0
    for rr in all_map.values():
        pid0 = int(rr["id"])
        if pid0 in snap_map:
            company_P_sum += float(snap_map[pid0]["final_project_score"] or 0.0)
        else:
            company_P_sum += float(_calc_final_score_row(db, rr) or 0.0)
    company_score_sum = _trunc1(company_P_sum)

    # 4) 사용자가 평가한 프로젝트(해당 기간, 최신 평가 1건/프로젝트)
    user_eval = db.execute(
        text(
            """
            SELECT DISTINCT ON (pe.project_id)
              pe.project_id,
              pe.created_at,
              COALESCE(pe.score,0)::float AS score
            FROM project_evaluations pe
            WHERE pe.user_id = :uid
              AND pe.created_at >= :s AND pe.created_at < :e
            ORDER BY pe.project_id, pe.created_at DESC, pe.id DESC
            """
        ),
        {"uid": user_id, "s": start_dt, "e": end_dt},
    ).mappings().all()

    user_proj_ids = [int(r["project_id"]) for r in user_eval]
    if not user_proj_ids:
        # 회사 값만, 직원 값은 0
        return PerformanceSummary(company_project_count=len(proj_ids), company_project_score_sum=company_score_sum), []

    # 5) 프로젝트 리스트 + 합계
    projects_out: List[ProjectRow] = []
    emp_score_sum = 0.0  # 참여 프로젝트 P 합
    alloc_sum = 0.0

    for ue in user_eval:
        pid = int(ue["project_id"])
        base = all_map.get(pid)
        if not base:
            continue
        if pid in snap_map:
            # 사업완료 스냅샷이 있으면: 프로젝트 총점=final_project_score, 환산점수=converted_score(없으면 산식으로 계산)
            P = float(snap_map[pid]["final_project_score"] or 0.0)
            score = float(item_map.get(pid, {}).get("user_eval_score", ue.get("score") or 0.0) or 0.0)
            alloc = float(item_map.get(pid, {}).get("converted_score", _trunc1((P / 10.0) * score)) or 0.0)
        else:
            P = float(_calc_final_score_row(db, base) or 0.0)
            score = float(ue.get("score") or 0.0)
            alloc = _trunc1((P / 10.0) * score)

        projects_out.append(
            ProjectRow(
                project_id=pid,
                project_name=str(base.get("name") or f"#{pid}"),
                evaluated_at=(snap_map.get(pid, {}).get("completed_at") if pid in snap_map else ue["created_at"]),
                project_final_score=P,
                personal_score=score,
                allocated_score=alloc,
                score_source=("SNAPSHOT" if pid in snap_map else "LIVE"),
            )
        )
        emp_score_sum += P
        alloc_sum += alloc

    # 개인이 참여한 프로젝트에서의 점유율(%): alloc_sum / emp_score_sum
    share = 0.0 if emp_score_sum <= 0 else _trunc1((alloc_sum / emp_score_sum) * 100.0)

    summary = PerformanceSummary(
        company_project_count=len(proj_ids),
        company_project_score_sum=company_score_sum,
        employee_project_count=len(projects_out),
        employee_project_score_sum=_trunc1(emp_score_sum),
        employee_allocated_score_sum=_trunc1(alloc_sum),
        employee_share_percent=share,
    )
    return summary, projects_out

def _calc_attendance(db: Session, user_id: int, start_dt: dt.datetime, end_dt: dt.datetime) -> AttendanceSummary:
    if not _table_exists(db, "attendance_records"):
        return AttendanceSummary()

    # ENUM 안전: COALESCE(shift_type::text,'') 형태로
    rows = db.execute(
        text(
            """
            SELECT work_date AS work_date,
                   check_in_at AS check_in_at,
                   check_out_at AS check_out_at,
                   COALESCE(shift_type::text, '') AS shift_type,
                   COALESCE(is_holiday_work, false) AS is_holiday_work,
                   COALESCE(status::text, '') AS status
            FROM attendance_records
            WHERE user_id = :uid
              AND work_date >= CAST(:s AS timestamp)::date
              AND work_date < CAST(:e AS timestamp)::date
              AND deleted_at IS NULL
            """
        ),
        {"uid": user_id, "s": start_dt, "e": end_dt},
    ).mappings().all()

    if not rows:
        return AttendanceSummary()

    # 총근무일(기록이 있는 날짜)
    total_days = len({str(r["work_date"]) for r in rows})

    # 근무시간(분)
    total_minutes = 0
    for r in rows:
        ci = r.get("check_in_at")
        co = r.get("check_out_at")
        if ci and co:
            try:
                diff = (co - ci).total_seconds() / 60.0
                if diff > 0:
                    total_minutes += int(diff)
            except Exception:
                pass
    total_hours = round(total_minutes / 60.0, 2)

    office = sum(1 for r in rows if str(r.get("shift_type") or "").upper() == "OFFICE")
    offsite = sum(1 for r in rows if str(r.get("shift_type") or "").upper() == "OUTSIDE")
    leave = sum(1 for r in rows if str(r.get("shift_type") or "").upper() == "LEAVE")
    half_leave = sum(1 for r in rows if "HALF" in str(r.get("shift_type") or "").upper())

    holiday_work = sum(1 for r in rows if bool(r.get("is_holiday_work")))
    overtime = sum(1 for r in rows if "OVERTIME" in str(r.get("status") or "").upper() or "OVERTIME" in str(r.get("shift_type") or "").upper())
    extra = sum(1 for r in rows if "EXTRA" in str(r.get("status") or "").upper() or "ADDITIONAL" in str(r.get("status") or "").upper())

    # 실근무일수: leave/half 제외, checkin/out 있으면 근무 인정
    actual = 0
    for r in rows:
        st = str(r.get("shift_type") or "").upper()
        if st == "LEAVE" or "HALF" in st:
            continue
        if r.get("check_in_at") and r.get("check_out_at"):
            actual += 1

    avg = 0.0 if actual <= 0 else round(total_hours / actual, 2)

    return AttendanceSummary(
        total_days=int(total_days),
        actual_work_days=int(actual),
        total_work_hours=float(total_hours),
        avg_work_hours=float(avg),
        office_days=int(office),
        offsite_days=int(offsite),
        annual_leave_days=float(leave),
        half_leave_days=float(half_leave) * 0.5,
        overtime_days=int(overtime),
        holiday_work_days=int(holiday_work),
        extra_work_days=int(extra),
    )


@router.get("/report", response_model=StaffReportOut)
def get_staff_report(
    unit: str = Query(..., description="month|year"),
    date: str = Query(..., description="YYYY-MM or YYYY"),
    user_id: int = Query(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> StaffReportOut:
    _require_admin(db, current_user)

    start_dt, end_dt = _period_range(unit, date)
    emp_name, dept_name = _load_employee_info(db, user_id)

    perf, projects = _calc_project_performance(db, user_id, start_dt, end_dt)
    att = _calc_attendance(db, user_id, start_dt, end_dt)

    return StaffReportOut(
        unit=unit,
        date=date,
        user_id=int(user_id),
        employee_name=emp_name,
        department_name=dept_name,
        performance=perf,
        attendance=att,
        projects=projects,
    )