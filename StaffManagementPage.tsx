import React, { useEffect, useMemo, useState } from "react";
import dayjs from "dayjs";

type StaffItem = { id: number; name: string; department_name?: string | null };

type PerformanceSummary = {
  company_project_count: number;
  company_project_score_sum: number;

  employee_project_count: number;
  employee_project_score_sum: number;
  employee_allocated_score_sum: number;
  employee_share_percent: number; // 0~100
};

type AttendanceSummary = {
  total_days: number;        // 총근무일
  actual_work_days: number;  // 실근무일수
  total_work_hours: number;
  avg_work_hours: number;

  office_days: number;
  offsite_days: number;
  annual_leave_days: number;
  half_leave_days: number;
  overtime_days: number;
  holiday_work_days: number;
  extra_work_days: number;
};

type ProjectRow = {
  project_id: number;
  project_name: string;
  evaluated_at: string;
  project_final_score: number;
  personal_score: number;
  allocated_score: number;
};

type StaffReport = {
  unit: "month" | "year";
  date: string;
  user_id: number;
  employee_name: string;
  department_name?: string | null;

  performance: PerformanceSummary;
  attendance: AttendanceSummary;
  projects: ProjectRow[];
};

function truncTo(n: number, digits: number) {
  const f = Math.pow(10, digits);
  return Math.trunc(n * f) / f;
}

function num(v: any, digits = 2) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "-";
  const t = truncTo(n, digits);
  return t.toLocaleString(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

async function apiJson<T>(url: string, options?: RequestInit): Promise<T> {
  const raw =
    localStorage.getItem("uplink_access_token") ||
    localStorage.getItem("uplink_token") ||
    localStorage.getItem("access_token") ||
    localStorage.getItem("token") ||
    sessionStorage.getItem("uplink_access_token") ||
    sessionStorage.getItem("uplink_token") ||
    sessionStorage.getItem("access_token") ||
    sessionStorage.getItem("token") ||
    "";

  let token = raw;
  if (token && (token.startsWith("{") || token.startsWith("["))) {
    try {
      const obj: any = JSON.parse(token);
      token = obj.uplink_token || obj.access_token || obj.token || raw;
    } catch {}
  }

  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...(options?.headers as any),
  };
  if (!headers["Authorization"] && token) {
    headers["Authorization"] = token.startsWith("Bearer ")
      ? token
      : `Bearer ${token}`;
  }

  const res = await fetch(url, { credentials: "include", ...options, headers });
  if (!res.ok) throw new Error((await res.text()) || `HTTP ${res.status}`);
  return res.json();
}

export default function StaffManagementPage() {
  const [unit, setUnit] = useState<"month" | "year">("month");
  const [selectedMonth, setSelectedMonth] = useState(dayjs().format("YYYY-MM"));
  const [selectedYear, setSelectedYear] = useState(dayjs().format("YYYY"));

  const [staff, setStaff] = useState<StaffItem[]>([]);
  const [userId, setUserId] = useState<number>(0);

  const [loadingStaff, setLoadingStaff] = useState(false);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [report, setReport] = useState<StaffReport | null>(null);

  const dateValue = unit === "month" ? selectedMonth : selectedYear;

  const years = useMemo(() => {
    const cur = dayjs().year();
    return Array.from({ length: 9 }).map((_, i) => String(cur - 4 + i));
  }, []);

  const loadStaff = async () => {
    setLoadingStaff(true);
    try {
      const r: any = await apiJson<any>("/api/admin/users");
      const list = Array.isArray(r) ? r : (r?.items ?? r?.data ?? r?.users ?? []);
      const mapped: StaffItem[] = (list || [])
        .map((u: any) => ({
          id: Number(u.id ?? u.user_id ?? u.employee_id),
          name: String(u.name ?? u.username ?? u.full_name ?? u.employee_name ?? ""),
          department_name: u.department_name ?? null,
        }))
        .filter((x: any) => Number.isFinite(x.id) && x.id > 0 && x.name);

      mapped.sort((a, b) => a.id - b.id);

      setStaff(mapped);
      setUserId((prev) => (prev && mapped.some((m) => m.id === prev) ? prev : (mapped[0]?.id ?? 0)));
    } finally {
      setLoadingStaff(false);
    }
  };

  const loadReport = async () => {
    if (!userId) return;
    setLoading(true);
    setErr(null);
    try {
      const r = await apiJson<StaffReport>(
        `/api/admin/staff/report?unit=${unit}&date=${encodeURIComponent(dateValue)}&user_id=${userId}`
      );
      setReport(r);
    } catch (e: any) {
      setReport(null);
      setErr(String(e?.message ?? e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { loadStaff().catch(() => {}); }, []);
  useEffect(() => {
    if (staff.length > 0 && userId > 0) loadReport().catch(() => {});
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [staff.length, userId]);

  return (
    <div style={{ padding: 16 }}>
      <style>{`
        input[type="month"]::-webkit-calendar-picker-indicator {
          filter: invert(1) brightness(1.6);
        }

        .uplink-card{
          background: rgba(18,24,38,0.92);
          border: 1px solid rgba(255,255,255,0.14);
          border-radius: 16px;
          padding: 14px;
          color: rgba(255,255,255,0.92);
          box-shadow: 0 0 0 1px rgba(120,160,255,0.15) inset;
        }
        .uplink-title{font-size:20px;font-weight:950;letter-spacing:-0.2px;}
        .uplink-sub{font-size:12px;opacity:0.75;font-weight:800;}
        .uplink-input{
          height:38px;padding:0 12px;border-radius:12px;
          border:1px solid rgba(255,255,255,0.18);
          background:rgba(255,255,255,0.06);color:white;font-weight:900;box-sizing:border-box;
        }
        .uplink-btn{
          height:38px;padding:0 14px;border-radius:12px;
          border:1px solid rgba(255,255,255,0.18);
          background:rgba(255,255,255,0.08);color:white;font-weight:950;cursor:pointer;
        }
        .uplink-btn.primary{background:rgba(120,160,255,0.25);border-color:rgba(120,160,255,0.55);}
        .uplink-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;}
        @media (max-width: 980px){.uplink-grid{grid-template-columns:1fr;}}
        .kv{display:grid;grid-template-columns:210px 1fr;gap:14px;
          padding:10px 12px;border-radius:14px;background:rgba(0,0,0,0.18);
          border:1px solid rgba(255,255,255,0.10);
        }
        .k{font-size:12px;opacity:0.75;font-weight:900;white-space:nowrap;}
        .v{font-size:14px;font-weight:950;}
        table{width:100%;border-collapse:collapse;}
        th{text-align:left;font-size:12px;font-weight:950;padding:10px 12px;border-bottom:1px solid rgba(255,255,255,0.12);white-space:nowrap;opacity:0.9;}
        td{font-size:13px;padding:10px 12px;border-bottom:1px solid rgba(255,255,255,0.08);vertical-align:top;}
        td.num{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap;}
        td.mono{font-variant-numeric:tabular-nums;white-space:nowrap;}
      `}</style>

      <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", gap: 10, flexWrap: "wrap" }}>
        <div>
          <div className="uplink-title">운영관리 · 직원관리</div>
          <div className="uplink-sub">직원별 성과(프로젝트 배분점수) + 근태 요약을 기간별로 확인합니다.</div>
        </div>
      </div>

      <div className="uplink-card" style={{ marginTop: 12 }}>
        <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
          <select className="uplink-input" value={unit} onChange={(e) => setUnit(e.target.value as any)} style={{ width: 120, backgroundColor: "#1f2937" }}>
            <option value="month">월별</option>
            <option value="year">연간</option>
          </select>

          {unit === "month" ? (
            <input className="uplink-input" type="month" value={selectedMonth} onChange={(e) => setSelectedMonth(e.target.value)} />
          ) : (
            <select className="uplink-input" value={selectedYear} onChange={(e) => setSelectedYear(e.target.value)} style={{ width: 130, backgroundColor: "#1f2937" }}>
              {years.map((y) => (<option key={y} value={y}>{y}년</option>))}
            </select>
          )}

          <select
            className="uplink-input"
            value={userId}
            onChange={(e) => setUserId(Number(e.target.value))}
            style={{ width: 300, backgroundColor: "#1f2937" }}
            disabled={loadingStaff || staff.length === 0}
          >
            {staff.length === 0 ? (
              <option value={0}>(직원 목록 없음)</option>
            ) : (
              staff.map((s) => (
                <option key={s.id} value={s.id}>
                  {s.name}{s.department_name ? ` · ${s.department_name}` : ""}
				  
                </option>
              ))
            )}
          </select>

          <button className="uplink-btn primary" onClick={() => loadReport()} disabled={loading || !userId}>
            {loading ? "조회중..." : "조회"}
          </button>
        </div>
      </div>

      {err && (
        <div className="uplink-card" style={{ marginTop: 12, borderColor: "rgba(255,80,80,0.55)" }}>
          <div style={{ fontWeight: 950, marginBottom: 6, color: "rgba(255,80,80,0.95)" }}>오류</div>
          <div style={{ whiteSpace: "pre-wrap", fontSize: 13, lineHeight: 1.5 }}>{err}</div>
        </div>
      )}

      {report && (
        <>
          <div className="uplink-grid" style={{ marginTop: 12 }}>
            <div className="uplink-card">
              <div style={{ fontWeight: 950, marginBottom: 10 }}>섹션 1 · 프로젝트 성과</div>
              <div className="kv">
                <div className="k">회사 총 사업 수</div><div className="v">{num(report.performance.company_project_count, 0)}</div>
                <div className="k">회사 총 프로젝트 총점 합</div><div className="v">{num(report.performance.company_project_score_sum, 1)}</div>
                <div className="k">직원 참여 사업 수</div><div className="v">{num(report.performance.employee_project_count, 0)}</div>
                <div className="k">참여 프로젝트 회사 점수 합</div><div className="v">{num(report.performance.employee_project_score_sum, 1)}</div>
                
				<div className="k">개인 환산 점수 합</div>
<div className="v">{num(report.performance.employee_allocated_score_sum, 1)}</div>

{/* ✅ 추가 */}
<div className="k">회사 내에서 개인 점유율(%)</div>
<div className="v">
  {(
    Number(report.performance.company_project_score_sum) > 0
      ? (Number(report.performance.employee_allocated_score_sum) /
          Number(report.performance.company_project_score_sum)) *
        100
      : 0
  ).toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 })}
  %
</div>

{/* ✅ 이름만 변경 */}
<div className="k">개인이 참여한 프로젝트에서의 점유율(%)</div>
<div className="v">{num(report.performance.employee_share_percent, 1)}%</div>


              </div>
            </div>

            <div className="uplink-card">
              <div style={{ fontWeight: 950, marginBottom: 10 }}>섹션 2 · 근태 요약</div>
              <div className="kv">
                <div className="k">총근무일</div><div className="v">{report.attendance.total_days}</div>
                <div className="k">실근무일수</div><div className="v">{report.attendance.actual_work_days}</div>
                <div className="k">총 근무시간</div><div className="v">{num(report.attendance.total_work_hours, 2)}h</div>
                <div className="k">평균 근무시간</div><div className="v">{num(report.attendance.avg_work_hours, 2)}h</div>
                <div className="k">사무실 / 외근</div><div className="v">{report.attendance.office_days} / {report.attendance.offsite_days}</div>
                <div className="k">월차 / 반차</div><div className="v">{num(report.attendance.annual_leave_days, 1)} / {num(report.attendance.half_leave_days, 1)}</div>
                <div className="k">야근 / 휴일근무 / 추가업무</div><div className="v">{report.attendance.overtime_days} / {report.attendance.holiday_work_days} / {report.attendance.extra_work_days}</div>
              </div>
            </div>
          </div>

          <div className="uplink-card" style={{ marginTop: 12 }}>
            <div style={{ fontWeight: 950, marginBottom: 10 }}>참여 프로젝트 리스트</div>
            <div style={{ overflowX: "auto" }}>
              <table>
                <thead>
                  <tr>
                    <th>평가확정일</th>
                    <th>프로젝트</th>
                    <th style={{ textAlign: "right" }}>프로젝트 총점</th>
                    <th style={{ textAlign: "right" }}>개인 점수</th>
                    <th style={{ textAlign: "right" }}>환산 점수</th>
                  </tr>
                </thead>
                <tbody>
                  {(report.projects || []).map((p) => (
                    <tr key={p.project_id}>
                      <td className="mono">{dayjs(p.evaluated_at).isValid() ? dayjs(p.evaluated_at).format("YYYY-MM-DD") : p.evaluated_at}</td>
                      <td style={{ fontWeight: 950 }}>{p.project_name}</td>
                      <td className="num">{num(p.project_final_score, 1)}</td>
                      <td className="num">{num(p.personal_score, 1)}</td>
                      <td className="num">{num(p.allocated_score, 1)}</td>
                    </tr>
                  ))}
                  {(report.projects || []).length === 0 && (
                    <tr><td colSpan={5} style={{ opacity: 0.8 }}>해당 기간에 평가 확정된 프로젝트가 없습니다.</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
