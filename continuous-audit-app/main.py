import hashlib
import ast
import json
import os
import re
import time
import uuid
from datetime import datetime
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

import db
import validation

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
# Environment-driven so the same code serves sandbox and production.
# Override CA_CATALOG / CA_SCHEMA in app.yaml to flip environments.
CATALOG = os.getenv("CA_CATALOG", "sandbox")
SCHEMA  = os.getenv("CA_SCHEMA",  "grc")
T_CFG   = f"{CATALOG}.{SCHEMA}.tb_test_configurations"
T_HIST  = f"{CATALOG}.{SCHEMA}.tb_test_configurations_history"
T_EXEC  = f"{CATALOG}.{SCHEMA}.tb_tests_executions"

# External reference tables — configurable via env vars
# Override in app.yaml if your paths differ:
#   COMPLIANCE_RISKS_TABLE: "compliance.your_schema.tb_risks"
#   COMPLIANCE_AREAS_TABLE: "compliance.your_schema.tb_areas"
#   COMPLIANCE_ENTRIES_TABLE: "compliance.your_schema.tb_risk_entries"
T_RISKS   = os.getenv("COMPLIANCE_RISKS_TABLE",   "compliance.sharepoint_list.tb_risks")
T_ENTRIES      = os.getenv("COMPLIANCE_ENTRIES_TABLE", "compliance.sharepoint_list.tb_risk_entries")
T_SUPPRESSIONS = f"{CATALOG}.{SCHEMA}.tb_test_suppressions"
T_HASHES       = f"{CATALOG}.{SCHEMA}.tb_incident_hashes"
T_AREAS   = os.getenv("COMPLIANCE_AREAS_TABLE",   "compliance.sharepoint_list.tb_areas")

SELF_REVIEW_ALLOWED = {
    "fernando.baptista@cerc.com",
}

ORCHESTRATOR_JOB_ID = os.getenv("ORCHESTRATOR_JOB_ID", "")

T_DASH_VIEWS  = f"{CATALOG}.{SCHEMA}.tb_dashboard_views"
T_DASH_CHARTS = f"{CATALOG}.{SCHEMA}.tb_dashboard_charts"

VALID_CHART_TYPES = {"line", "bar", "bar_h", "pie", "kpi"}
VALID_DATE_RANGES = {"7d", "30d", "90d", "all"}
VALID_Y_AGGS      = {"COUNT", "SUM", "AVG", "MIN", "MAX"}
VALID_WIDTHS      = {"half", "full"}
_COL_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

app = FastAPI(title="Continuous Audit V2", docs_url="/api/docs")


# ─────────────────────────────────────────────────────────────────────────────
# DB session middleware — reuse ONE warehouse connection per /api request (#1).
# Pure-ASGI (not BaseHTTPMiddleware) so the contextvar it sets propagates to the
# route handler. The connection is opened lazily on first query, so requests
# that don't touch the DB (static/frontend) never open one.
# ─────────────────────────────────────────────────────────────────────────────
class DBSessionMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http" or not scope.get("path", "").startswith("/api/"):
            await self.app(scope, receive, send)
            return
        holder = {"conn": None}
        token = db._request_db.set(holder)
        try:
            await self.app(scope, receive, send)
        finally:
            db._request_db.reset(token)
            if holder["conn"] is not None:
                try:
                    await run_in_threadpool(holder["conn"].close)
                except Exception:
                    pass


app.add_middleware(DBSessionMiddleware)


# ─────────────────────────────────────────────────────────────────────────────
# Startup migrations — pending-review columns for editing ACTIVE tests (#1)
# The approved version keeps running while a proposed change waits for review.
# ─────────────────────────────────────────────────────────────────────────────
_PENDING_COLUMNS = [
    ("pending_query_type",              "STRING"),
    ("pending_imports",                 "STRING"),
    ("pending_query_code",              "STRING"),
    ("pending_threshold",               "INT"),
    ("pending_frequency",               "STRING"),
    ("pending_description",             "STRING"),
    ("pending_responsible_area",        "STRING"),
    ("pending_risco_id",                "STRING"),
    ("pending_category",                "STRING"),
    ("pending_should_activate_channel", "BOOLEAN"),
    ("pending_submitted_by",            "STRING"),
    ("pending_submitted_at",            "TIMESTAMP"),
    ("has_pending_review",              "BOOLEAN"),
]


def _ensure_pending_columns() -> None:
    """Idempotent: add each pending_* column if it doesn't exist yet."""
    for name, typ in _PENDING_COLUMNS:
        try:
            db.execute(f"ALTER TABLE {T_CFG} ADD COLUMNS ({name} {typ})")
        except Exception:
            pass  # column already exists — fine


@app.on_event("startup")
def _startup_migrations() -> None:
    try:
        _ensure_pending_columns()
    except Exception:
        pass  # never let a migration hiccup block the app from serving


# ─────────────────────────────────────────────────────────────────────────────
# User
# ─────────────────────────────────────────────────────────────────────────────
class User(BaseModel):
    email: str
    name:  str


def get_user(
    x_forwarded_email: Optional[str] = Header(None),
    x_forwarded_user:  Optional[str] = Header(None),
) -> User:
    email = x_forwarded_email or os.getenv("DEV_EMAIL", "dev@local")
    name  = (x_forwarded_user or email.split("@")[0]).replace(".", " ").title()
    return User(email=email, name=name)


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic models
# ─────────────────────────────────────────────────────────────────────────────
class TestIn(BaseModel):
    test_name:               str
    output_table:            str
    description:             Optional[str] = ""
    responsible_area:        Optional[str] = ""
    risco_id:                Optional[str] = "N/A"
    threshold:               int           = 0
    frequency:               str           = "DAILY"
    query_type:              str
    imports:                 Optional[str] = ""
    query_code:              str
    category:                Optional[str] = ""
    should_activate_channel: bool          = True


class RejectIn(BaseModel):
    reason: str


class SuppressionIn(BaseModel):
    linked_entry_id:    str
    linked_entry_title: Optional[str] = ""
    note:               Optional[str] = ""


class RunPreviewIn(BaseModel):
    query_type: str
    imports:    Optional[str] = ""
    query_code: str


class DashViewIn(BaseModel):
    title: str


class DashChartIn(BaseModel):
    title:      str
    chart_type: str           # line | bar | bar_h | pie | kpi
    test_id:    str
    width:      str = "half"  # half | full
    config:     dict          # x_axis, y_aggregation, y_column, group_by, top_n_series, date_range, palette


class MoveIn(BaseModel):
    direction: str  # up | down


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _insert_history(test_id, test_name, version, query_type, imports,
                    query_code, status_before, status_after, change_type,
                    changed_by, comment=None):
    db.execute(f"""
        INSERT INTO {T_HIST} (
            history_id, test_id, test_name, version, query_type, imports,
            query_code, status_before, status_after, changed_by, changed_at,
            change_type, comment
        ) VALUES (
            %(history_id)s, %(test_id)s, %(test_name)s, %(version)s,
            %(query_type)s, %(imports)s, %(query_code)s, %(status_before)s,
            %(status_after)s, %(changed_by)s, %(changed_at)s,
            %(change_type)s, %(comment)s
        )
    """, {
        "history_id": str(uuid.uuid4()), "test_id": test_id,
        "test_name": test_name, "version": version,
        "query_type": query_type, "imports": imports or "",
        "query_code": query_code, "status_before": status_before,
        "status_after": status_after, "changed_by": changed_by,
        "changed_at": datetime.utcnow(), "change_type": change_type,
        "comment": comment,
    })


def _require_test(test_id: str) -> dict:
    rows = db.query(f"SELECT * FROM {T_CFG} WHERE test_id = %(id)s", {"id": test_id})
    if not rows:
        raise HTTPException(404, "Teste não encontrado")
    return rows[0]


def _safe_table_name(name: str) -> str:
    if not re.match(r'^[a-z0-9_]+$', name):
        raise HTTPException(400, "Nome da tabela inválido")
    return name


def _safe_fallback(fn):
    """Execute fn(), return [] on any error (for external reference tables)."""
    try:
        return fn()
    except Exception:
        return []


# Small in-process TTL cache for reference data that rarely changes (#3).
_REF_CACHE_TTL = 300  # seconds — bump/lower here if needed
_ref_cache: dict = {}


def _cached(key: str, fn):
    """Return a cached value if fresh; otherwise compute, cache (only on success) and return."""
    now = time.time()
    hit = _ref_cache.get(key)
    if hit and hit[1] > now:
        return hit[0]
    val = fn()
    if isinstance(val, list):  # never cache an {"error": ...} payload
        _ref_cache[key] = (val, now + _REF_CACHE_TTL)
    return val


# ─────────────────────────────────────────────────────────────────────────────
# Routes — user
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/user")
def me(user: User = Depends(get_user)):
    return {
        "email": user.email,
        "name":  user.name,
        "can_self_review": user.email in SELF_REVIEW_ALLOWED,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Routes — reference data
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/health")
def health(user: User = Depends(get_user)):
    """Diagnostic endpoint — tests DB connectivity and returns table row counts."""
    results = {}
    for name, table in [
        ("configurations", T_CFG),
        ("executions", T_EXEC),
        ("suppressions", T_SUPPRESSIONS),
        ("hashes", T_HASHES),
    ]:
        try:
            rows = db.query(f"SELECT COUNT(*) AS cnt FROM {table}")
            results[name] = rows[0]["cnt"]
        except Exception as e:
            results[name] = f"ERROR: {str(e)[:100]}"
    return {"status": "ok", "tables": results, "user": user.email}


@app.get("/api/risks")
def list_risks(user: User = Depends(get_user)):
    def _load():
        try:
            return db.query(f"SELECT RiskId, RiskTitle FROM {T_RISKS} ORDER BY RiskId")
        except Exception as e:
            return {"error": f"[{T_RISKS}] {str(e)}", "data": []}
    return _cached("risks", _load)


@app.get("/api/areas")
def list_areas(user: User = Depends(get_user)):
    def _load():
        try:
            return db.query(f"SELECT DISTINCT Area FROM {T_AREAS} WHERE Area IS NOT NULL ORDER BY Area")
        except Exception as e:
            return {"error": f"[{T_AREAS}] {str(e)}", "data": []}
    return _cached("areas", _load)


# ─────────────────────────────────────────────────────────────────────────────
# Routes — stats
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/stats")
def get_stats(user: User = Depends(get_user)):
    try:
        counts = db.query(f"SELECT status, COUNT(*) AS cnt FROM {T_CFG} GROUP BY status")
        sm = {r["status"]: r["cnt"] for r in counts}
    except Exception:
        sm = {}

    try:
        exec_stats = db.query(f"""
            SELECT TestResult, COUNT(*) AS cnt FROM {T_EXEC}
            WHERE ExecutionDate >= date_sub(current_timestamp(), 30)
            GROUP BY TestResult
        """)
        em = {r["TestResult"]: r["cnt"] for r in exec_stats}
    except Exception:
        em = {}

    # Current alert status of each active test (for cards)
    try:
        status_rows = db.query(f"""
            SELECT
                CASE
                    WHEN lr.TestResult IS NULL       THEN 'nunca_rodou'
                    WHEN lr.TestResult = 'PASSED'    THEN 'sem_achados'
                    WHEN lr.TestResult = 'ERROR'     THEN 'erro'
                    WHEN sup.suppression_id IS NOT NULL AND lr.TestResult = 'FAILED' THEN 'em_tratamento'
                    WHEN lr.IsRecurrent = true       THEN 'reincidente'
                    WHEN lr.IsContinued = true       THEN 'persistente'
                    WHEN lr.TestResult = 'FAILED'    THEN 'novo_achado'
                    ELSE 'sem_achados'
                END AS alert_status,
                COUNT(*) AS cnt
            FROM {T_CFG} c
            LEFT JOIN (
                SELECT TestName, TestResult, IsSupressed, IsRecurrent, IsContinued,
                    ROW_NUMBER() OVER (PARTITION BY TestName ORDER BY ExecutionDate DESC) AS rn
                FROM {T_EXEC}
            ) lr ON lr.TestName = c.test_name AND lr.rn = 1
            LEFT JOIN (
                SELECT test_id, MIN(suppression_id) AS suppression_id
                FROM {T_SUPPRESSIONS} WHERE active = true GROUP BY test_id
            ) sup ON sup.test_id = c.test_id
            WHERE c.status = 'ACTIVE'
            GROUP BY alert_status
        """)
        sc = {r["alert_status"]: r["cnt"] for r in status_rows}
    except Exception:
        sc = {}

    return {
        "active":        sm.get("ACTIVE", 0),
        "paused":        sm.get("PAUSED", 0),
        "under_review":  sm.get("UNDER_REVIEW", 0) + sm.get("PENDING_DELETE", 0),
        "cleared":       em.get("PASSED", 0),
        "flagged":       em.get("FAILED", 0),
        "error":         em.get("ERROR", 0),
        "by_alert":      sc,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Routes — dashboard
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/dashboard")
def get_dashboard(
    date_from: Optional[str] = None,
    date_to:   Optional[str] = None,
    result:    Optional[str] = None,
    area:      Optional[str] = None,
    test_name: Optional[str] = None,
    notify:    Optional[str] = None,
    user: User = Depends(get_user),
):
    # Build exec filter (aliased to `e` — every runs_* query joins config as `c`)
    exec_filters = ["1=1"]
    exec_params  = {}
    if date_from:
        exec_filters.append("e.ExecutionDate >= %(date_from)s"); exec_params["date_from"] = date_from
    if date_to:
        exec_filters.append("e.ExecutionDate <= %(date_to)s");   exec_params["date_to"]   = date_to
    if result:
        exec_filters.append("e.TestResult = %(result)s");        exec_params["result"]    = result
    exec_where = " AND ".join(exec_filters)

    # Build cfg filter
    cfg_filters = ["1=1"]
    cfg_params  = {}
    if area:
        cfg_filters.append("c.responsible_area = %(area)s");   cfg_params["area"]      = area
    if test_name:
        cfg_filters.append("c.test_name = %(test_name)s");     cfg_params["test_name"] = test_name
    if notify == "true":
        cfg_filters.append("c.should_activate_channel = true")
    elif notify == "false":
        cfg_filters.append("c.should_activate_channel = false")
    cfg_where = " AND ".join(cfg_filters)

    # Coverage KPIs (current snapshot) — one query, conditional COUNT DISTINCT (#2).
    # Respect the config filters (area/test/notify) but not the date range.
    _cov = db.query(f"""
        SELECT
            COUNT(*) AS total_tests,
            COUNT(DISTINCT CASE WHEN c.risco_id IS NOT NULL AND c.risco_id != 'N/A' THEN c.risco_id END) AS risks_covered,
            COUNT(DISTINCT CASE WHEN c.responsible_area IS NOT NULL THEN c.responsible_area END) AS areas_covered
        FROM {T_CFG} c WHERE c.status='ACTIVE' AND {cfg_where}
    """, cfg_params)[0]
    total_tests   = _cov["total_tests"]
    risks_covered = _cov["risks_covered"]
    areas_covered = _cov["areas_covered"]

    # Execution KPIs — one query with conditional aggregation (#2). Joins config so
    # area/test/notify filters apply to runs too.
    _runs_from   = f"FROM {T_EXEC} e JOIN {T_CFG} c ON c.test_name = e.TestName"
    _runs_params = {**exec_params, **cfg_params}
    _rc = db.query(f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN e.TestResult='PASSED' THEN 1 ELSE 0 END) AS cleared,
            SUM(CASE WHEN e.TestResult='FAILED' THEN 1 ELSE 0 END) AS flagged,
            SUM(CASE WHEN e.TestResult='ERROR'  THEN 1 ELSE 0 END) AS error
        {_runs_from} WHERE {exec_where} AND {cfg_where}
    """, _runs_params)[0]
    runs_total   = _rc["total"]   or 0
    runs_cleared = _rc["cleared"] or 0
    runs_flagged = _rc["flagged"] or 0
    runs_error   = _rc["error"]   or 0

    # By area
    by_area = db.query(f"""
        SELECT c.responsible_area AS area, COUNT(*) AS cnt
        FROM {T_CFG} c
        WHERE c.status = 'ACTIVE' AND {cfg_where}
        GROUP BY c.responsible_area ORDER BY cnt DESC
    """, cfg_params)

    # By risk (join with tb_risks for title)
    by_risk = _safe_fallback(lambda: db.query(f"""
        SELECT
            c.risco_id,
            COALESCE(r.RiskTitle, c.risco_id) AS risk_title,
            COUNT(*) AS cnt
        FROM {T_CFG} c
        LEFT JOIN {T_RISKS} r ON r.RiskId = c.risco_id
        WHERE c.status = 'ACTIVE' AND c.risco_id IS NOT NULL AND c.risco_id != 'N/A' AND {cfg_where}
        GROUP BY c.risco_id, r.RiskTitle ORDER BY cnt DESC LIMIT 20
    """, cfg_params))

    # Active tests whose linked risk entry is still open (derived from suppressions,
    # which are the real link between a test and a risk entry — ClosingDate = open/closed).
    tests_with_open_entries = _safe_fallback(lambda: db.query(f"""
        SELECT
            c.test_name,
            c.risco_id,
            c.responsible_area,
            COUNT(DISTINCT s.linked_entry_id) AS open_entries
        FROM {T_SUPPRESSIONS} s
        JOIN {T_CFG} c     ON c.test_id = s.test_id AND c.status = 'ACTIVE'
        JOIN {T_ENTRIES} e ON e.RiskEntryId = s.linked_entry_id
        WHERE s.active = true
          AND (e.ClosingDate IS NULL OR TRIM(COALESCE(e.ClosingDate, '')) = '')
        GROUP BY c.test_name, c.risco_id, c.responsible_area
        ORDER BY open_entries DESC
        LIMIT 20
    """))

    # (runs_error is now computed together with the other run counts above — #2)

    # Test-centric view: last result per active test
    by_test_status = db.query(f"""
        SELECT
            c.test_name,
            c.responsible_area,
            c.risco_id,
            lr.TestResult    AS last_result,
            GREATEST(0, COALESCE(lr.IncidentCountRaw, lr.IncidentCount, 0) - COALESCE(fp_agg.fp_count, 0))
                AS last_incidents,
            lr.ExecutionDate AS last_run,
            COUNT(e.TestResult) AS total_runs,
            SUM(CASE WHEN e.TestResult = 'FAILED' THEN 1 ELSE 0 END) AS flagged_runs
        FROM {T_CFG} c
        LEFT JOIN (
            SELECT TestName, TestResult, IncidentCount, IncidentCountRaw, ExecutionDate,
                ROW_NUMBER() OVER (PARTITION BY TestName ORDER BY ExecutionDate DESC) AS rn
            FROM {T_EXEC}
        ) lr ON lr.TestName = c.test_name AND lr.rn = 1
        LEFT JOIN (
            SELECT test_name, COUNT(*) AS fp_count
            FROM {CATALOG}.{SCHEMA}.tb_false_positives
            WHERE active = true
            GROUP BY test_name
        ) fp_agg ON fp_agg.test_name = c.test_name
        LEFT JOIN {T_EXEC} e ON e.TestName = c.test_name
            {"AND e.ExecutionDate >= %(date_from)s" if date_from else ""}
            {"AND e.ExecutionDate <= %(date_to)s" if date_to else ""}
        WHERE c.status = 'ACTIVE' AND {cfg_where}
        GROUP BY c.test_name, c.responsible_area, c.risco_id, lr.TestResult, lr.IncidentCount,
                 lr.IncidentCountRaw, lr.ExecutionDate, fp_agg.fp_count
        ORDER BY flagged_runs DESC, c.test_name
    """, {**cfg_params, **(exec_params)})

    return {
        "kpis": {
            "total_tests":    total_tests,
            "risks_covered":  risks_covered,
            "areas_covered":  areas_covered,
            "runs_total":     runs_total,
            "runs_cleared":   runs_cleared,
            "runs_flagged":   runs_flagged,
            "runs_error":     runs_error,
        },
        "by_area":                by_area,
        "by_risk":                by_risk,
        "by_test_status":         by_test_status,
        "tests_with_open_entries": tests_with_open_entries,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Routes — run preview
# ─────────────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
# Routes — risk entries (for suppression linking)
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/risk-entries")
def search_risk_entries(q: str = "", open_only: bool = True, user: User = Depends(get_user)):
    """Search risk entries by RiskEntryId or title. open_only=True filters to open entries."""
    try:
        where_clauses = []
        if q:
            where_clauses.append(f"(RiskEntryId LIKE '%{q}%' OR RiskEntryTitle LIKE '%{q}%')")
        if open_only:
            where_clauses.append("(ClosingDate IS NULL OR TRIM(COALESCE(ClosingDate,'')) = '')")
        where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        return db.query(f"""
            SELECT RiskEntryId, RiskEntryTitle, Criticality, ResponsibleArea,
                   OpeningDate, ClosingDate, ImplementationDeadline
            FROM {T_ENTRIES}
            {where}
            ORDER BY RiskEntryId
            LIMIT 50
        """)
    except Exception as e:
        return {"error": f"[{T_ENTRIES}] {str(e)}", "data": []}


# ─────────────────────────────────────────────────────────────────────────────
# Routes — suppressions
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/tests/{test_id}/suppressions")
def list_suppressions(test_id: str, user: User = Depends(get_user)):
    """List all suppressions for a test, including current entry status."""
    try:
        return db.query(f"""
            SELECT
                s.*,
                e.RiskEntryTitle  AS entry_title,
                e.Criticality     AS entry_criticality,
                e.ClosingDate     AS entry_closing_date,
                CASE
                    WHEN (e.ClosingDate IS NULL OR TRIM(COALESCE(e.ClosingDate,'')) = '')
                    THEN true ELSE false
                END AS entry_is_open
            FROM {T_SUPPRESSIONS} s
            LEFT JOIN {T_ENTRIES} e ON e.RiskEntryId = s.linked_entry_id
            WHERE s.test_id = %(test_id)s
            ORDER BY s.created_at DESC
        """, {"test_id": test_id})
    except Exception as e:
        return {"error": str(e), "data": []}


@app.post("/api/tests/{test_id}/suppressions", status_code=201)
def create_suppression(test_id: str, body: SuppressionIn, user: User = Depends(get_user)):
    test = _require_test(test_id)
    now  = datetime.utcnow()
    suppression_id = str(uuid.uuid4())
    db.execute(f"""
        INSERT INTO {T_SUPPRESSIONS}
            (suppression_id, test_id, test_name, linked_entry_id, linked_entry_title, note, created_by, created_at, active)
        VALUES
            (%(id)s, %(test_id)s, %(test_name)s, %(entry_id)s, %(entry_title)s, %(note)s, %(by)s, %(now)s, true)
    """, {
        "id":          suppression_id,
        "test_id":     test_id,
        "test_name":   test["test_name"],
        "entry_id":    body.linked_entry_id,
        "entry_title": body.linked_entry_title or "",
        "note":        body.note or "",
        "by":          user.email,
        "now":         now,
    })
    return {"suppression_id": suppression_id}


@app.delete("/api/suppressions/{suppression_id}")
def deactivate_suppression(suppression_id: str, user: User = Depends(get_user)):
    db.execute(f"""
        UPDATE {T_SUPPRESSIONS} SET active = false
        WHERE suppression_id = %(id)s
    """, {"id": suppression_id})
    return {"deactivated": True}


# ─────────────────────────────────────────────────────────────────────────────
# Routes — incident hashes (for continuity/recurrence display)
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/tests/{test_id}/hashes")
def get_hashes(test_id: str, user: User = Depends(get_user)):
    test = _require_test(test_id)
    return db.query(f"""
        SELECT execution_date, row_count, is_suppressed, is_recurrent,
               LEFT(incident_hash, 8) AS hash_preview
        FROM {T_HASHES}
        WHERE test_name = %(name)s
        ORDER BY execution_date DESC
        LIMIT 30
    """, {"name": test["test_name"]})


@app.post("/api/run-preview")
def run_preview(body: RunPreviewIn, user: User = Depends(get_user)):
    qt = (body.query_type or "").upper()
    if qt == "SQL":
        try:
            sql = body.query_code.strip().rstrip(";")
            rows = db.query(f"SELECT * FROM ({sql}) __preview LIMIT 10")
            cols = list(rows[0].keys()) if rows else []
            return {"success": True, "row_count": len(rows), "columns": cols, "sample": rows}
        except Exception as e:
            return {"success": False, "error": str(e)}
    elif qt == "PYTHON":
        try:
            ast.parse(f"{body.imports or ''}\n{body.query_code}")
            return {"success": True, "message": "Sintaxe OK — o código não é executado aqui; a execução real ocorre no orquestrador. Garanta que o resultado é atribuído a `df_incidents`."}
        except SyntaxError as e:
            return {"success": False, "error": f"Erro de sintaxe na linha {e.lineno}: {e.msg}"}
    return {"success": False, "error": f"Tipo desconhecido: {qt}"}


# ─────────────────────────────────────────────────────────────────────────────
# Routes — tests
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/tests")
def list_tests(user: User = Depends(get_user)):
    try:
        return db.query(f"""
            SELECT
                c.*,
                lr.TestResult    AS last_result,
                lr.ExecutionDate AS last_run,
                GREATEST(0, COALESCE(lr.IncidentCountRaw, lr.IncidentCount, 0) - COALESCE(fp_agg.fp_count, 0))
                    AS last_incident_count,
                CASE WHEN sup.suppression_id IS NOT NULL THEN true ELSE false END AS has_active_suppression,
                CASE
                    WHEN lr.TestResult IS NULL       THEN 'nunca_rodou'
                    WHEN lr.TestResult = 'PASSED'    THEN 'sem_achados'
                    WHEN lr.TestResult = 'ERROR'     THEN 'erro'
                    WHEN sup.suppression_id IS NOT NULL AND lr.TestResult = 'FAILED' THEN 'em_tratamento'
                    WHEN lr.IsRecurrent = true       THEN 'reincidente'
                    WHEN lr.IsContinued = true       THEN 'persistente'
                    WHEN lr.TestResult = 'FAILED'    THEN 'novo_achado'
                    ELSE 'sem_achados'
                END AS last_alert_status
            FROM {T_CFG} c
            LEFT JOIN (
                SELECT TestName, TestResult, ExecutionDate, IncidentCount, IncidentCountRaw,
                    IsSupressed, IsRecurrent, IsContinued,
                    ROW_NUMBER() OVER (PARTITION BY TestName ORDER BY ExecutionDate DESC) AS rn
                FROM {T_EXEC}
            ) lr ON lr.TestName = c.test_name AND lr.rn = 1
            LEFT JOIN (
                SELECT test_id, MIN(suppression_id) AS suppression_id
                FROM {T_SUPPRESSIONS} WHERE active = true GROUP BY test_id
            ) sup ON sup.test_id = c.test_id
            LEFT JOIN (
                SELECT test_name, COUNT(*) AS fp_count
                FROM {CATALOG}.{SCHEMA}.tb_false_positives
                WHERE active = true
                GROUP BY test_name
            ) fp_agg ON fp_agg.test_name = c.test_name
            ORDER BY c.updated_at DESC
        """)
    except Exception as e1:
        # Fallback: simple query without suppression join
        try:
            rows = db.query(f"""
                SELECT c.*,
                    lr.TestResult    AS last_result,
                    lr.ExecutionDate AS last_run,
                    GREATEST(0, COALESCE(lr.IncidentCount, 0) - COALESCE(fp_agg.fp_count, 0))
                        AS last_incident_count,
                    false            AS has_active_suppression,
                    CASE
                        WHEN lr.TestResult IS NULL       THEN 'nunca_rodou'
                        WHEN lr.TestResult = 'PASSED'    THEN 'sem_achados'
                        WHEN lr.TestResult = 'ERROR'     THEN 'erro'
                        WHEN lr.IsSupressed = true       THEN 'em_tratamento'
                        WHEN lr.IsRecurrent = true       THEN 'reincidente'
                        WHEN lr.IsContinued = true       THEN 'persistente'
                        WHEN lr.TestResult = 'FAILED'    THEN 'novo_achado'
                        ELSE 'sem_achados'
                    END AS last_alert_status
                FROM {T_CFG} c
                LEFT JOIN (
                    SELECT TestName, TestResult, ExecutionDate, IncidentCount,
                        IsSupressed, IsRecurrent, IsContinued,
                        ROW_NUMBER() OVER (PARTITION BY TestName ORDER BY ExecutionDate DESC) AS rn
                    FROM {T_EXEC}
                ) lr ON lr.TestName = c.test_name AND lr.rn = 1
                LEFT JOIN (
                    SELECT test_name, COUNT(*) AS fp_count
                    FROM {CATALOG}.{SCHEMA}.tb_false_positives
                    WHERE active = true
                    GROUP BY test_name
                ) fp_agg ON fp_agg.test_name = c.test_name
                ORDER BY c.updated_at DESC
            """)
            return rows
        except Exception as e2:
            # Last resort: just return configs without execution data
            return db.query(f"SELECT * FROM {T_CFG} ORDER BY updated_at DESC")


@app.get("/api/tests/{test_id}")
def get_test(test_id: str, user: User = Depends(get_user)):
    return _require_test(test_id)


@app.get("/api/tests/{test_id}/history")
def get_history(test_id: str, user: User = Depends(get_user)):
    return db.query(
        f"SELECT * FROM {T_HIST} WHERE test_id = %(id)s ORDER BY changed_at DESC",
        {"id": test_id}
    )


@app.get("/api/tests/{test_id}/executions")
def get_executions(test_id: str, user: User = Depends(get_user)):
    test = _require_test(test_id)
    return db.query(
        f"SELECT * FROM {T_EXEC} WHERE TestName = %(name)s ORDER BY ExecutionDate DESC LIMIT 20",
        {"name": test["test_name"]}
    )


def _row_hash(row: dict) -> str:
    """SHA256 of business fields — excludes ArchiveDate and all _-prefixed internal columns."""
    content_str = "|".join(
        str(v) if v is not None else ""
        for k, v in sorted(row.items())
        if k != "ArchiveDate" and not k.startswith("_")
    )
    return hashlib.sha256(content_str.encode("utf-8")).hexdigest()


@app.get("/api/tests/{test_id}/incidents")
def get_incidents(
    test_id:   str,
    date_from: Optional[str] = None,   # YYYY-MM-DD
    date_to:   Optional[str] = None,
    search:    Optional[str] = None,   # free text search across all string columns
    all_dates: bool = False,           # True → return every execution (no date filter)
    limit:     int = 500,
    user: User = Depends(get_user),
):
    test = _require_test(test_id)
    table = _safe_table_name(test["output_table"])
    full_table = f"{CATALOG}.{SCHEMA}.{table}"
    search = (search or "").strip()
    try:
        # ── Date filter (parameterized) ──────────────────────────────────────────
        params: dict = {}
        if all_dates:
            date_filter = "1=1"
        elif date_from and date_to:
            date_filter = "DATE(ArchiveDate) BETWEEN %(date_from)s AND %(date_to)s"
            params["date_from"] = date_from; params["date_to"] = date_to
        elif date_from:
            date_filter = "DATE(ArchiveDate) >= %(date_from)s"; params["date_from"] = date_from
        elif date_to:
            date_filter = "DATE(ArchiveDate) <= %(date_to)s"; params["date_to"] = date_to
        elif search:
            # Searching with no explicit range → look across the whole history (#10)
            date_filter = "1=1"
        else:
            # Default: latest execution date
            date_filter = f"DATE(ArchiveDate) = (SELECT DATE(MAX(ArchiveDate)) FROM {full_table})"

        # ── Server-side free-text search across business columns (#10) ───────────
        search_where = ""
        if search:
            try:
                probe = db.query(f"SELECT * FROM {full_table} LIMIT 1")
                scols = [c for c in (probe[0].keys() if probe else [])
                         if c != "ArchiveDate" and not c.startswith("_")]
            except Exception:
                scols = []
            if scols:
                concat = "concat_ws(' ', " + ", ".join(f"CAST(`{c}` AS STRING)" for c in scols) + ")"
                search_where = f"AND lower({concat}) LIKE lower(%(q)s)"
                params["q"] = f"%{search}%"

        # Get available dates for the date picker
        dates = db.query(f"""
            SELECT DISTINCT DATE(ArchiveDate) AS exec_date
            FROM {full_table}
            ORDER BY exec_date DESC
            LIMIT 30
        """)

        rows = db.query(f"""
            SELECT * FROM {full_table}
            WHERE {date_filter} {search_where}
            ORDER BY ArchiveDate DESC
            LIMIT {limit}
        """, params)

        # Load active FPs (criteria-based, with legacy row_hash fallback)
        try:
            fp_rows = db.query(
                f"SELECT fp_id, row_hash, match_criteria, note, marked_by, marked_at "
                f"FROM {CATALOG}.{SCHEMA}.tb_false_positives "
                f"WHERE test_name = %(n)s AND active = true",
                {"n": test["test_name"]}
            )
        except Exception:
            # match_criteria column may not exist yet — fall back to legacy schema
            try:
                fp_rows = db.query(
                    f"SELECT fp_id, row_hash, note, marked_by, marked_at "
                    f"FROM {CATALOG}.{SCHEMA}.tb_false_positives "
                    f"WHERE test_name = %(n)s AND active = true",
                    {"n": test["test_name"]}
                )
            except Exception:
                fp_rows = []

        # Pre-parse criteria JSON once
        fps = []
        for fr in fp_rows:
            criteria = None
            raw = fr.get("match_criteria")
            if raw:
                try:
                    criteria = json.loads(raw)
                except Exception:
                    criteria = None
            fps.append({**fr, "_criteria": criteria})

        for row in rows:
            row["_row_hash"] = _row_hash(row)
            matched = None
            for fp in fps:
                if fp.get("_criteria"):
                    if _row_matches_criteria(row, fp["_criteria"]):
                        matched = fp; break
                elif fp.get("row_hash") and fp["row_hash"] == row["_row_hash"]:
                    matched = fp; break
            if matched:
                row["_is_false_positive"] = True
                row["_fp_id"]        = matched.get("fp_id")
                row["_fp_note"]      = matched.get("note", "")
                row["_fp_marked_by"] = matched.get("marked_by", "")
                row["_fp_marked_at"] = matched.get("marked_at")
            else:
                row["_is_false_positive"] = False
                row["_fp_id"]        = None
                row["_fp_note"]      = None
                row["_fp_marked_by"] = None
                row["_fp_marked_at"] = None

        fp_count = sum(1 for row in rows if row["_is_false_positive"])

        cols = [c for c in (rows[0].keys() if rows else []) if not c.startswith("_")]

        # Compare latest two hashes
        try:
            hash_rows = db.query(
                f"SELECT incident_hash, is_suppressed, is_recurrent FROM {T_HASHES} WHERE test_name = %(n)s ORDER BY execution_date DESC LIMIT 2",
                {"n": test["test_name"]}
            )
            is_same = (len(hash_rows) >= 2 and hash_rows[0].get("incident_hash") and
                       hash_rows[0]["incident_hash"] == hash_rows[1]["incident_hash"])
            last_meta = hash_rows[0] if hash_rows else None
        except Exception:
            is_same = False
            last_meta = None

        return {
            "columns":             cols,
            "rows":                rows,
            "count":               len(rows),
            "fp_count":            fp_count,
            "exec_dates":          [str(d["exec_date"]) for d in dates],
            "is_same_as_previous": is_same,
            "last_hash_meta":      last_meta,
            "search":              search or None,
            "limited":             len(rows) >= limit,
        }
    except Exception as e:
        raise HTTPException(400, f"Erro ao consultar achados: {str(e)}")


# ─────────────────────────────────────────────────────────────────────────────
# Routes — create / edit
# ─────────────────────────────────────────────────────────────────────────────
def _validate_required_fields(body: TestIn, submit: bool):
    """Validates required fields. On submit, all must be present."""
    errors = []
    if not body.test_name or not body.test_name.strip():
        errors.append("Nome do Teste é obrigatório")
    if not body.output_table or not body.output_table.startswith("tb_incidents_"):
        errors.append("Tabela de Achados inválida — deve começar com 'tb_incidents_' e ter um sufixo")
    if submit:
        if not body.responsible_area or not body.responsible_area.strip():
            errors.append("Área Responsável é obrigatória")
        if not body.risco_id or not body.risco_id.strip() or body.risco_id == "N/A":
            errors.append("ID do Risco é obrigatório")
        if not body.query_code or not body.query_code.strip():
            errors.append("O código da query é obrigatório")
        if not body.query_type or body.query_type not in ("SQL", "PYTHON"):
            errors.append("Tipo de Query inválido")
    if errors:
        raise HTTPException(400, " | ".join(errors))


@app.post("/api/tests", status_code=201)
def create_test(body: TestIn, submit: bool = False, user: User = Depends(get_user)):
    _validate_required_fields(body, submit)
    if submit:
        try:
            validation.validate(body.query_type, body.imports, body.query_code)
        except ValueError as e:
            raise HTTPException(400, str(e))

    test_id = str(uuid.uuid4())
    now     = datetime.utcnow()
    status  = "UNDER_REVIEW" if submit else "DRAFT"

    db.execute(f"""
        INSERT INTO {T_CFG} (
            test_id, test_name, output_table, description, responsible_area,
            risco_id, threshold, frequency, query_type, imports, query_code,
            status, category, created_by, created_at, updated_at, version,
            should_activate_channel
        ) VALUES (
            %(test_id)s, %(test_name)s, %(output_table)s, %(description)s,
            %(responsible_area)s, %(risco_id)s, %(threshold)s, %(frequency)s,
            %(query_type)s, %(imports)s, %(query_code)s, %(status)s,
            %(category)s, %(created_by)s, %(now)s, %(now)s, 1,
            %(should_activate_channel)s
        )
    """, {
        "test_id": test_id, "test_name": body.test_name,
        "output_table": body.output_table, "description": body.description,
        "responsible_area": body.responsible_area, "risco_id": body.risco_id,
        "threshold": body.threshold, "frequency": body.frequency,
        "query_type": body.query_type, "imports": body.imports,
        "query_code": body.query_code, "status": status,
        "category": body.category, "created_by": user.email, "now": now,
        "should_activate_channel": body.should_activate_channel,
    })

    _insert_history(test_id, body.test_name, 1, body.query_type, body.imports,
                    body.query_code, None, status,
                    "SUBMITTED_FOR_REVIEW" if submit else "CREATED", user.email)
    return {"test_id": test_id, "status": status}


@app.put("/api/tests/{test_id}")
def edit_test(test_id: str, body: TestIn, submit: bool = False, user: User = Depends(get_user)):
    test = _require_test(test_id)

    # DRAFT/REJECTED: full edit, freely (draft or submit for review)
    # ACTIVE: any change becomes a PENDING edit that requires approval — the approved
    #         (live) version keeps running until a reviewer approves the change (#1)
    # Other statuses: not editable
    if test["status"] not in ("DRAFT", "REJECTED", "ACTIVE"):
        raise HTTPException(400, f"Testes com status '{test['status']}' não podem ser editados")

    now = datetime.utcnow()
    is_active = test["status"] == "ACTIVE"

    # ── ACTIVE: stage a pending change; never touch the live version ──────────────
    if is_active:
        # An edit to an active test always goes to review, so enforce full validation.
        _validate_required_fields(body, submit=True)
        try:
            validation.validate(body.query_type, body.imports, body.query_code)
        except ValueError as e:
            raise HTTPException(400, str(e))

        db.execute(f"""
            UPDATE {T_CFG} SET
                pending_query_type              = %(query_type)s,
                pending_imports                 = %(imports)s,
                pending_query_code              = %(query_code)s,
                pending_threshold               = %(threshold)s,
                pending_frequency               = %(frequency)s,
                pending_description             = %(description)s,
                pending_responsible_area        = %(responsible_area)s,
                pending_risco_id                = %(risco_id)s,
                pending_category                = %(category)s,
                pending_should_activate_channel = %(should_activate_channel)s,
                pending_submitted_by            = %(by)s,
                pending_submitted_at            = %(now)s,
                has_pending_review              = true,
                updated_at                      = %(now)s
            WHERE test_id = %(test_id)s
        """, {
            "query_type": body.query_type, "imports": body.imports,
            "query_code": body.query_code, "threshold": body.threshold,
            "frequency": body.frequency, "description": body.description,
            "responsible_area": body.responsible_area, "risco_id": body.risco_id,
            "category": body.category,
            "should_activate_channel": body.should_activate_channel,
            "by": user.email, "now": now, "test_id": test_id,
        })
        _insert_history(test_id, test["test_name"], test["version"], body.query_type,
                        body.imports, body.query_code, "ACTIVE", "ACTIVE",
                        "EDITED_PENDING_REVIEW", user.email)
        return {"test_id": test_id, "status": "ACTIVE", "pending_review": True}

    # ── DRAFT/REJECTED: edit the row directly ────────────────────────────────────
    _validate_required_fields(body, submit)
    if submit:
        try:
            validation.validate(body.query_type, body.imports, body.query_code)
        except ValueError as e:
            raise HTTPException(400, str(e))

    new_version = int(test["version"] or 1) + 1
    status = "UNDER_REVIEW" if submit else "DRAFT"

    if not body.output_table.startswith("tb_incidents_"):
        raise HTTPException(400, "O nome da tabela deve começar com 'tb_incidents_'")

    db.execute(f"""
        UPDATE {T_CFG} SET
            test_name               = %(test_name)s,
            output_table            = %(output_table)s,
            description             = %(description)s,
            responsible_area        = %(responsible_area)s,
            risco_id                = %(risco_id)s,
            threshold               = %(threshold)s,
            frequency               = %(frequency)s,
            query_type              = %(query_type)s,
            imports                 = %(imports)s,
            query_code              = %(query_code)s,
            status                  = %(status)s,
            category                = %(category)s,
            should_activate_channel = %(should_activate_channel)s,
            updated_at              = %(now)s,
            version                 = %(version)s,
            rejection_reason        = NULL
        WHERE test_id = %(test_id)s
    """, {
        "test_name": body.test_name, "output_table": body.output_table,
        "description": body.description, "responsible_area": body.responsible_area,
        "risco_id": body.risco_id, "threshold": body.threshold,
        "frequency": body.frequency, "query_type": body.query_type,
        "imports": body.imports, "query_code": body.query_code,
        "status": status, "category": body.category,
        "should_activate_channel": body.should_activate_channel,
        "now": now, "version": new_version, "test_id": test_id,
    })

    _insert_history(test_id, body.test_name, new_version, body.query_type,
                    body.imports, body.query_code, test["status"], status,
                    "SUBMITTED_FOR_REVIEW" if submit else "EDITED", user.email)
    return {"test_id": test_id, "status": status}


# ─────────────────────────────────────────────────────────────────────────────
# Routes — state transitions
# ─────────────────────────────────────────────────────────────────────────────
@app.post("/api/tests/{test_id}/approve")
def approve_test(test_id: str, user: User = Depends(get_user)):
    test = _require_test(test_id)
    now = datetime.utcnow()

    # Pending edit on an ACTIVE test → promote the proposed version into the live one.
    if test.get("has_pending_review"):
        submitter = test.get("pending_submitted_by")
        if submitter == user.email and user.email not in SELF_REVIEW_ALLOWED:
            raise HTTPException(403, "Você não pode aprovar uma alteração que submeteu")
        new_version = int(test["version"] or 1) + 1
        db.execute(f"""
            UPDATE {T_CFG} SET
                query_type              = pending_query_type,
                imports                 = pending_imports,
                query_code              = pending_query_code,
                threshold               = pending_threshold,
                frequency               = pending_frequency,
                description             = pending_description,
                responsible_area        = pending_responsible_area,
                risco_id                = pending_risco_id,
                category                = pending_category,
                should_activate_channel = pending_should_activate_channel,
                version                 = %(v)s,
                reviewed_by             = %(r)s,
                updated_at              = %(n)s,
                rejection_reason        = NULL,
                has_pending_review              = false,
                pending_query_type              = NULL,
                pending_imports                 = NULL,
                pending_query_code              = NULL,
                pending_threshold               = NULL,
                pending_frequency               = NULL,
                pending_description             = NULL,
                pending_responsible_area        = NULL,
                pending_risco_id                = NULL,
                pending_category                = NULL,
                pending_should_activate_channel = NULL,
                pending_submitted_by            = NULL,
                pending_submitted_at            = NULL
            WHERE test_id = %(id)s
        """, {"v": new_version, "r": user.email, "n": now, "id": test_id})
        _insert_history(test_id, test["test_name"], new_version,
                        test.get("pending_query_type"), test.get("pending_imports"),
                        test.get("pending_query_code"), "ACTIVE", "ACTIVE",
                        "APPROVED", user.email)
        return {"status": "ACTIVE", "promoted": True}

    if test["status"] not in ("UNDER_REVIEW", "PENDING_DELETE"):
        raise HTTPException(400, "Status inválido para aprovação")
    if test["created_by"] == user.email and user.email not in SELF_REVIEW_ALLOWED:
        raise HTTPException(403, "Você não pode aprovar um teste que criou")

    if test["status"] == "PENDING_DELETE":
        db.execute(f"UPDATE {T_CFG} SET status='CANCELLED', reviewed_by=%(r)s, updated_at=%(n)s WHERE test_id=%(id)s",
                   {"r": user.email, "n": now, "id": test_id})
        _insert_history(test_id, test["test_name"], test["version"], test["query_type"],
                        test["imports"], test["query_code"], "PENDING_DELETE", "CANCELLED", "APPROVED", user.email)
        return {"status": "CANCELLED"}

    db.execute(f"UPDATE {T_CFG} SET status='ACTIVE', reviewed_by=%(r)s, activated_at=%(n)s, updated_at=%(n)s WHERE test_id=%(id)s",
               {"r": user.email, "n": now, "id": test_id})
    _insert_history(test_id, test["test_name"], test["version"], test["query_type"],
                    test["imports"], test["query_code"], "UNDER_REVIEW", "ACTIVE", "APPROVED", user.email)
    return {"status": "ACTIVE"}


@app.post("/api/tests/{test_id}/reject")
def reject_test(test_id: str, body: RejectIn, user: User = Depends(get_user)):
    test = _require_test(test_id)
    now  = datetime.utcnow()

    # Reject a pending edit → discard the proposal; the live version keeps running.
    if test.get("has_pending_review"):
        submitter = test.get("pending_submitted_by")
        if submitter == user.email and user.email not in SELF_REVIEW_ALLOWED:
            raise HTTPException(403, "Você não pode rejeitar uma alteração que submeteu")
        db.execute(f"""
            UPDATE {T_CFG} SET
                has_pending_review              = false,
                pending_query_type              = NULL,
                pending_imports                 = NULL,
                pending_query_code              = NULL,
                pending_threshold               = NULL,
                pending_frequency               = NULL,
                pending_description             = NULL,
                pending_responsible_area        = NULL,
                pending_risco_id                = NULL,
                pending_category                = NULL,
                pending_should_activate_channel = NULL,
                pending_submitted_by            = NULL,
                pending_submitted_at            = NULL,
                rejection_reason                = %(rr)s,
                reviewed_by                     = %(r)s,
                updated_at                      = %(n)s
            WHERE test_id = %(id)s
        """, {"rr": body.reason, "r": user.email, "n": now, "id": test_id})
        _insert_history(test_id, test["test_name"], test["version"], test["query_type"],
                        test["imports"], test["query_code"], "ACTIVE", "ACTIVE",
                        "REJECTED", user.email, body.reason)
        return {"status": "ACTIVE", "pending_rejected": True}

    if test["status"] not in ("UNDER_REVIEW", "PENDING_DELETE"):
        raise HTTPException(400, "Status inválido para rejeição")
    if test["created_by"] == user.email and user.email not in SELF_REVIEW_ALLOWED:
        raise HTTPException(403, "Você não pode rejeitar um teste que criou")

    new_status = "ACTIVE" if test["status"] == "PENDING_DELETE" else "REJECTED"
    db.execute(f"UPDATE {T_CFG} SET status=%(s)s, reviewed_by=%(r)s, rejection_reason=%(rr)s, updated_at=%(n)s WHERE test_id=%(id)s",
               {"s": new_status, "r": user.email, "rr": body.reason, "n": now, "id": test_id})
    _insert_history(test_id, test["test_name"], test["version"], test["query_type"],
                    test["imports"], test["query_code"], test["status"], new_status, "REJECTED", user.email, body.reason)
    return {"status": new_status}


@app.post("/api/tests/{test_id}/request-delete")
def request_delete(test_id: str, body: RejectIn, user: User = Depends(get_user)):
    """
    ACTIVE/PAUSED: goes to PENDING_DELETE (needs approval).
    REJECTED: cancelled immediately (already reviewed, no need for re-approval).
    Body.reason is the justification provided by the requester.
    """
    test = _require_test(test_id)

    if test["status"] not in ("ACTIVE", "PAUSED", "REJECTED", "DRAFT"):
        raise HTTPException(400, f"Testes com status '{test['status']}' não podem ser excluídos")

    now = datetime.utcnow()

    # REJECTED and DRAFT: cancel directly, no approval needed
    if test["status"] in ("REJECTED", "DRAFT"):
        db.execute(f"UPDATE {T_CFG} SET status='CANCELLED', updated_at=%(n)s WHERE test_id=%(id)s",
                   {"n": now, "id": test_id})
        _insert_history(test_id, test["test_name"], test["version"], test["query_type"],
                        test["imports"], test["query_code"], test["status"], "CANCELLED",
                        "CANCELLED", user.email, body.reason)
        return {"status": "CANCELLED"}

    # ACTIVE/PAUSED: goes to approval queue
    db.execute(f"UPDATE {T_CFG} SET status='PENDING_DELETE', updated_at=%(n)s WHERE test_id=%(id)s",
               {"n": now, "id": test_id})
    _insert_history(test_id, test["test_name"], test["version"], test["query_type"],
                    test["imports"], test["query_code"], test["status"], "PENDING_DELETE",
                    "CANCELLED", user.email, body.reason)
    return {"status": "PENDING_DELETE"}


@app.post("/api/tests/{test_id}/pause")
def pause_test(test_id: str, user: User = Depends(get_user)):
    test = _require_test(test_id)
    if test["status"] != "ACTIVE":
        raise HTTPException(400, "Apenas testes ACTIVE podem ser pausados")
    now = datetime.utcnow()
    db.execute(f"UPDATE {T_CFG} SET status='PAUSED', updated_at=%(n)s WHERE test_id=%(id)s", {"n": now, "id": test_id})
    _insert_history(test_id, test["test_name"], test["version"], test["query_type"],
                    test["imports"], test["query_code"], "ACTIVE", "PAUSED", "PAUSED", user.email)
    return {"status": "PAUSED"}


@app.post("/api/tests/{test_id}/activate")
def activate_test(test_id: str, user: User = Depends(get_user)):
    test = _require_test(test_id)
    if test["status"] != "PAUSED":
        raise HTTPException(400, "Apenas testes PAUSED podem ser reativados")
    now = datetime.utcnow()
    db.execute(f"UPDATE {T_CFG} SET status='ACTIVE', updated_at=%(n)s WHERE test_id=%(id)s", {"n": now, "id": test_id})
    _insert_history(test_id, test["test_name"], test["version"], test["query_type"],
                    test["imports"], test["query_code"], "PAUSED", "ACTIVE", "REACTIVATED", user.email)
    return {"status": "ACTIVE"}


# ─────────────────────────────────────────────────────────────────────────────
# Routes — false positives
# ─────────────────────────────────────────────────────────────────────────────
class FalsePositiveIn(BaseModel):
    test_name:      str
    match_criteria: Optional[list] = None   # [{"column","value"}, ...] — 1–3 predicates, row matches if it satisfies ALL
    note:           Optional[str] = ""
    row_hash:       Optional[str] = None    # originating row hash (reference / legacy fallback)
    row_data:       Optional[dict] = None   # business columns of the originating row


def _normalize_criteria(raw) -> list:
    """Validate & normalize match_criteria into a list of 1–3 {column, value} predicates."""
    if not raw or not isinstance(raw, list):
        raise HTTPException(400, "Selecione de 1 a 3 critérios para o falso positivo")
    out = []
    for item in raw:
        col = str((item or {}).get("column", "")).strip()
        if not col or not _COL_RE.match(col):
            raise HTTPException(400, f"Coluna inválida no critério: {col!r}")
        val = (item or {}).get("value")
        out.append({"column": col, "value": "" if val is None else str(val)})
    if not (1 <= len(out) <= 3):
        raise HTTPException(400, "Selecione de 1 a 3 critérios para o falso positivo")
    cols = [c["column"] for c in out]
    if len(set(cols)) != len(cols):
        raise HTTPException(400, "Não repita a mesma coluna nos critérios")
    return out


def _row_matches_criteria(row: dict, criteria: list) -> bool:
    """True if the row satisfies ALL {column, value} predicates (string comparison, null → '')."""
    for c in criteria:
        want = "" if c.get("value") is None else str(c.get("value"))
        got  = row.get(c.get("column"))
        got  = "" if got is None else str(got)
        if got != want:
            return False
    return True


_FP_HISTORY_CREATE = f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.tb_false_positives_history (
        fp_history_id STRING,
        fp_id         STRING,
        event_type    STRING,
        test_name     STRING,
        row_hash      STRING,
        row_data      STRING,
        note          STRING,
        actor         STRING,
        event_at      TIMESTAMP
    )
"""

_FP_HISTORY_INSERT = (
    f"INSERT INTO {CATALOG}.{SCHEMA}.tb_false_positives_history "
    "(fp_history_id, fp_id, event_type, test_name, row_hash, row_data, note, actor, event_at) "
    "VALUES (%(hid)s, %(fid)s, %(et)s, %(tn)s, %(rh)s, %(rd)s, %(note)s, %(actor)s, %(now)s)"
)


def _log_fp_history(fp_id: str, test_name: str, row_hash: str,
                    row_data_json: str, event_type: str, note: str, actor: str) -> None:
    """Insert a history event. Creates the table on first use if it doesn't exist yet."""
    params = {"hid": str(uuid.uuid4()), "fid": fp_id, "et": event_type,
              "tn": test_name, "rh": row_hash, "rd": row_data_json,
              "note": note, "actor": actor, "now": datetime.utcnow()}
    try:
        db.execute(_FP_HISTORY_INSERT, params)
    except Exception:
        # Table probably doesn't exist yet — create it, then retry once
        try:
            db.execute(_FP_HISTORY_CREATE)
            db.execute(_FP_HISTORY_INSERT, params)
        except Exception:
            pass  # history is non-critical; never let it block the main action


@app.get("/api/tests/{test_id}/false-positives")
def list_false_positives(test_id: str, user: User = Depends(get_user)):
    test = _require_test(test_id)
    try:
        return db.query(
            f"SELECT * FROM {CATALOG}.{SCHEMA}.tb_false_positives WHERE test_name = %(n)s ORDER BY marked_at DESC",
            {"n": test["test_name"]}
        )
    except Exception as e:
        return {"error": str(e), "data": []}


_FP_ADD_CRITERIA_COL = (
    f"ALTER TABLE {CATALOG}.{SCHEMA}.tb_false_positives ADD COLUMNS (match_criteria STRING)"
)
_FP_INSERT = (
    f"INSERT INTO {CATALOG}.{SCHEMA}.tb_false_positives "
    "(fp_id, test_name, row_hash, match_criteria, marked_by, marked_at, note, active) "
    "VALUES (%(id)s, %(tn)s, %(rh)s, %(mc)s, %(by)s, %(now)s, %(note)s, true)"
)


@app.post("/api/false-positives", status_code=201)
def mark_false_positive(body: FalsePositiveIn, user: User = Depends(get_user)):
    if not body.note or not body.note.strip():
        raise HTTPException(400, "O comentário é obrigatório ao marcar um Falso Positivo")
    criteria = _normalize_criteria(body.match_criteria)
    fp_id = str(uuid.uuid4())
    params = {
        "id": fp_id, "tn": body.test_name, "rh": body.row_hash,
        "mc": json.dumps(criteria, ensure_ascii=False),
        "by": user.email, "now": datetime.utcnow(), "note": body.note or "",
    }
    try:
        db.execute(_FP_INSERT, params)
    except Exception:
        # match_criteria column probably doesn't exist yet — add it, then retry once
        try:
            db.execute(_FP_ADD_CRITERIA_COL)
        except Exception:
            pass
        db.execute(_FP_INSERT, params)
    # row_data lives only in history
    row_data_json = json.dumps(body.row_data or {}, default=str)
    _log_fp_history(fp_id, body.test_name, body.row_hash or "", row_data_json,
                    "marked", body.note or "", user.email)
    return {"fp_id": fp_id}


@app.delete("/api/false-positives/{fp_id}")
def unmark_false_positive(fp_id: str, user: User = Depends(get_user)):
    # Fetch existing record so we can log it fully in history
    try:
        fp_rows = db.query(
            f"SELECT * FROM {CATALOG}.{SCHEMA}.tb_false_positives WHERE fp_id = %(id)s",
            {"id": fp_id}
        )
        fp = fp_rows[0] if fp_rows else {}
    except Exception:
        fp = {}
    db.execute(
        f"UPDATE {CATALOG}.{SCHEMA}.tb_false_positives SET active = false WHERE fp_id = %(id)s",
        {"id": fp_id}
    )
    _log_fp_history(
        fp_id,
        fp.get("test_name", ""),
        fp.get("row_hash", ""),
        fp.get("row_data") or "{}",
        "unmarked",
        fp.get("note", ""),
        user.email,
    )
    return {"unmarked": True}


@app.get("/api/false-positives/history")
def get_fp_history(limit: int = 200, user: User = Depends(get_user)):
    try:
        return db.query(f"""
            SELECT * FROM {CATALOG}.{SCHEMA}.tb_false_positives_history
            ORDER BY event_at DESC
            LIMIT {limit}
        """)
    except Exception:
        return []  # Table doesn't exist yet — return empty list, not an error


@app.get("/api/false-positives")
def list_all_false_positives(user: User = Depends(get_user)):
    """Global list of all active false positives with match_criteria + row_data from history."""
    def _base_query(extra: str, join: str) -> str:
        return f"""
            SELECT
                fp.fp_id, fp.test_name, fp.row_hash, fp.note, fp.marked_by, fp.marked_at,
                tc.test_id, tc.description AS test_description,
                tc.responsible_area, tc.status AS test_status
                {extra}
            FROM {CATALOG}.{SCHEMA}.tb_false_positives fp
            LEFT JOIN {CATALOG}.{SCHEMA}.tb_test_configurations tc
                ON tc.test_name = fp.test_name
            {join}
            WHERE fp.active = true
            ORDER BY fp.marked_at DESC
        """

    _hist_join = f"""LEFT JOIN (
        SELECT fp_id, row_data
        FROM {CATALOG}.{SCHEMA}.tb_false_positives_history
        WHERE event_type = 'marked'
    ) h ON h.fp_id = fp.fp_id"""

    # Try progressively simpler queries so a missing column/table degrades gracefully
    attempts = [
        (", fp.match_criteria, h.row_data", _hist_join, None),
        (", fp.match_criteria",             "",         {"row_data": None}),
        (", h.row_data",                    _hist_join, {"match_criteria": None}),
        ("",                                "",         {"match_criteria": None, "row_data": None}),
    ]
    last_err = None
    for extra, join, defaults in attempts:
        try:
            rows = db.query(_base_query(extra, join))
            if defaults:
                for row in rows:
                    for k, v in defaults.items():
                        row[k] = v
            return {"items": rows, "count": len(rows)}
        except Exception as e:
            last_err = e
    raise HTTPException(400, f"Erro ao listar falsos positivos: {str(last_err)}")


# ─────────────────────────────────────────────────────────────────────────────
# Routes — orchestrator trigger
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/run-orchestrator")
def orchestrator_status(user: User = Depends(get_user)):
    return {"configured": bool(ORCHESTRATOR_JOB_ID)}


@app.post("/api/run-orchestrator")
def run_orchestrator(user: User = Depends(get_user)):
    """
    Dispara o job do orquestrador via Databricks Jobs API.
    Configure ORCHESTRATOR_JOB_ID em app.yaml para habilitar.
    """
    if not ORCHESTRATOR_JOB_ID:
        return {"status": "not_configured",
                "message": "Adicione ORCHESTRATOR_JOB_ID em app.yaml para habilitar o disparo manual."}
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        run = w.jobs.run_now(job_id=int(ORCHESTRATOR_JOB_ID))
        return {"status": "triggered", "run_id": run.run_id}
    except Exception as e:
        raise HTTPException(500, f"Erro ao disparar orquestrador: {str(e)}")


# ─────────────────────────────────────────────────────────────────────────────
# Routes — dashboard views & charts
# ─────────────────────────────────────────────────────────────────────────────
_VIEWS_CREATE = f"""
    CREATE TABLE IF NOT EXISTS {T_DASH_VIEWS} (
        view_id    STRING,
        title      STRING,
        position   INTEGER,
        created_by STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
"""

_CHARTS_CREATE = f"""
    CREATE TABLE IF NOT EXISTS {T_DASH_CHARTS} (
        chart_id   STRING,
        view_id    STRING,
        title      STRING,
        chart_type STRING,
        test_id    STRING,
        config     STRING,
        position   INTEGER,
        width      STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
"""

_VIEWS_INSERT = (
    f"INSERT INTO {T_DASH_VIEWS} "
    "(view_id, title, position, created_by, created_at, updated_at) "
    "VALUES (%(vid)s, %(title)s, %(pos)s, %(by)s, %(now)s, %(now)s)"
)

_CHARTS_INSERT = (
    f"INSERT INTO {T_DASH_CHARTS} "
    "(chart_id, view_id, title, chart_type, test_id, config, position, width, created_at, updated_at) "
    "VALUES (%(cid)s, %(vid)s, %(title)s, %(ct)s, %(tid)s, %(cfg)s, %(pos)s, %(width)s, %(now)s, %(now)s)"
)


def _validate_chart_body(chart_type: str, width: str, config: dict) -> None:
    if chart_type not in VALID_CHART_TYPES:
        raise HTTPException(400, f"Tipo de gráfico inválido: {chart_type}")
    if width not in VALID_WIDTHS:
        raise HTTPException(400, f"Largura inválida: {width}")
    dr = str(config.get("date_range", "30d"))
    if dr not in VALID_DATE_RANGES:
        raise HTTPException(400, f"Período inválido: {dr}")
    y_agg = str(config.get("y_aggregation", "COUNT")).upper()
    if y_agg not in VALID_Y_AGGS:
        raise HTTPException(400, f"Agregação inválida: {y_agg}")
    for field in ("x_axis", "y_column", "group_by"):
        val = config.get(field)
        if val and str(val) not in ("ArchiveDate", "none", "null", ""):
            if not _COL_RE.match(str(val)):
                raise HTTPException(400, f"Nome de coluna inválido em {field}: {val}")


def _build_y_expr(y_aggregation: str, y_column: str) -> str:
    agg = (y_aggregation or "COUNT").upper()
    col = (y_column or "").strip()
    if agg == "COUNT" or not col or col in ("none", "null", ""):
        return "COUNT(*)"
    elif agg == "SUM":
        return f"COALESCE(SUM({col}), 0)"
    elif agg == "AVG":
        return f"COALESCE(AVG(CAST({col} AS DOUBLE)), 0)"
    elif agg == "MIN":
        return f"MIN({col})"
    elif agg == "MAX":
        return f"MAX({col})"
    return "COUNT(*)"


@app.get("/api/tests/{test_id}/incident-columns")
def get_incident_columns(test_id: str, user: User = Depends(get_user)):
    """Return column names/types for the test's incident table (for chart axis selection)."""
    test = _require_test(test_id)
    table = _safe_table_name(test["output_table"])
    full_table = f"{CATALOG}.{SCHEMA}.{table}"
    try:
        rows = db.query(f"SELECT * FROM {full_table} LIMIT 1")
        if rows:
            row = rows[0]
            all_cols = [k for k in row.keys() if not k.startswith("_") and k != "ArchiveDate"]
            numeric_cols     = [k for k in all_cols if isinstance(row[k], (int, float)) and not isinstance(row[k], bool)]
            categorical_cols = [k for k in all_cols if k not in numeric_cols]
        else:
            desc = db.query(f"DESCRIBE TABLE {full_table}")
            all_cols = [r["col_name"] for r in desc
                        if r.get("col_name") and not r["col_name"].startswith("_")
                        and r["col_name"] != "ArchiveDate" and not r["col_name"].startswith("#")]
            numeric_cols, categorical_cols = [], all_cols
        return {
            "test_name":        test["test_name"],
            "output_table":     test["output_table"],
            "numeric_cols":     numeric_cols,
            "categorical_cols": categorical_cols,
        }
    except Exception as e:
        return {
            "test_name": test["test_name"], "output_table": test["output_table"],
            "numeric_cols": [], "categorical_cols": [], "error": str(e)[:200],
        }


# ── Views (pages/tabs) ────────────────────────────────────────────────────────
@app.get("/api/dashboard-views")
def list_dash_views(user: User = Depends(get_user)):
    try:
        return db.query(f"SELECT * FROM {T_DASH_VIEWS} ORDER BY position ASC, created_at ASC")
    except Exception:
        return []


@app.post("/api/dashboard-views", status_code=201)
def create_dash_view(body: DashViewIn, user: User = Depends(get_user)):
    if not body.title.strip():
        raise HTTPException(400, "O nome da view é obrigatório")
    try:
        cnt = db.query(f"SELECT COUNT(*) AS c FROM {T_DASH_VIEWS}")[0]["c"]
        if cnt >= 20:
            raise HTTPException(400, "Limite de 20 views atingido.")
    except HTTPException:
        raise
    except Exception:
        cnt = 0

    view_id = str(uuid.uuid4())
    now = datetime.utcnow()
    try:
        pos_rows = db.query(f"SELECT COALESCE(MAX(position), 0) AS mx FROM {T_DASH_VIEWS}")
        pos = (pos_rows[0]["mx"] or 0) + 1
    except Exception:
        pos = 1

    params = {"vid": view_id, "title": body.title.strip(), "pos": pos, "by": user.email, "now": now}
    try:
        db.execute(_VIEWS_INSERT, params)
    except Exception:
        try:
            db.execute(_VIEWS_CREATE)
            db.execute(_VIEWS_INSERT, params)
        except Exception as e:
            raise HTTPException(400, f"Erro ao criar view: {str(e)}")
    return {"view_id": view_id}


@app.put("/api/dashboard-views/{view_id}")
def update_dash_view(view_id: str, body: DashViewIn, user: User = Depends(get_user)):
    if not body.title.strip():
        raise HTTPException(400, "O nome da view é obrigatório")
    now = datetime.utcnow()
    try:
        db.execute(
            f"UPDATE {T_DASH_VIEWS} SET title=%(title)s, updated_at=%(now)s WHERE view_id=%(vid)s",
            {"title": body.title.strip(), "now": now, "vid": view_id}
        )
    except Exception as e:
        raise HTTPException(400, f"Erro ao atualizar view: {str(e)}")
    return {"updated": True}


@app.delete("/api/dashboard-views/{view_id}")
def delete_dash_view(view_id: str, user: User = Depends(get_user)):
    try:
        db.execute(f"DELETE FROM {T_DASH_CHARTS} WHERE view_id=%(id)s", {"id": view_id})
    except Exception:
        pass  # Charts table may not exist yet
    try:
        db.execute(f"DELETE FROM {T_DASH_VIEWS} WHERE view_id=%(id)s", {"id": view_id})
    except Exception as e:
        raise HTTPException(400, f"Erro ao excluir view: {str(e)}")
    return {"deleted": True}


# ── Charts (widgets within a view) ────────────────────────────────────────────
@app.get("/api/dashboard-views/{view_id}/charts")
def list_view_charts(view_id: str, user: User = Depends(get_user)):
    try:
        return db.query(
            f"SELECT * FROM {T_DASH_CHARTS} WHERE view_id=%(vid)s ORDER BY position ASC, created_at ASC",
            {"vid": view_id}
        )
    except Exception:
        return []


@app.post("/api/dashboard-views/{view_id}/charts", status_code=201)
def add_chart(view_id: str, body: DashChartIn, user: User = Depends(get_user)):
    if not body.title.strip():
        raise HTTPException(400, "O título do gráfico é obrigatório")
    _validate_chart_body(body.chart_type, body.width, body.config)

    try:
        cnt = db.query(
            f"SELECT COUNT(*) AS c FROM {T_DASH_CHARTS} WHERE view_id=%(vid)s", {"vid": view_id}
        )[0]["c"]
        if cnt >= 16:
            raise HTTPException(400, "Limite de 16 gráficos por view atingido.")
    except HTTPException:
        raise
    except Exception:
        cnt = 0

    chart_id = str(uuid.uuid4())
    now = datetime.utcnow()
    try:
        pos_rows = db.query(
            f"SELECT COALESCE(MAX(position), 0) AS mx FROM {T_DASH_CHARTS} WHERE view_id=%(vid)s",
            {"vid": view_id}
        )
        pos = (pos_rows[0]["mx"] or 0) + 1
    except Exception:
        pos = 1

    params = {
        "cid": chart_id, "vid": view_id, "title": body.title.strip(),
        "ct": body.chart_type, "tid": body.test_id,
        "cfg": json.dumps(body.config, ensure_ascii=False),
        "pos": pos, "width": body.width, "now": now,
    }
    try:
        db.execute(_CHARTS_INSERT, params)
    except Exception:
        try:
            db.execute(_CHARTS_CREATE)
            db.execute(_CHARTS_INSERT, params)
        except Exception as e:
            raise HTTPException(400, f"Erro ao adicionar gráfico: {str(e)}")
    return {"chart_id": chart_id}


@app.put("/api/dashboard-charts/{chart_id}")
def update_chart(chart_id: str, body: DashChartIn, user: User = Depends(get_user)):
    if not body.title.strip():
        raise HTTPException(400, "O título do gráfico é obrigatório")
    _validate_chart_body(body.chart_type, body.width, body.config)
    now = datetime.utcnow()
    try:
        db.execute(
            f"UPDATE {T_DASH_CHARTS} SET title=%(title)s, chart_type=%(ct)s, test_id=%(tid)s, "
            "config=%(cfg)s, width=%(width)s, updated_at=%(now)s WHERE chart_id=%(cid)s",
            {
                "title": body.title.strip(), "ct": body.chart_type, "tid": body.test_id,
                "cfg": json.dumps(body.config, ensure_ascii=False),
                "width": body.width, "now": now, "cid": chart_id,
            }
        )
    except Exception as e:
        raise HTTPException(400, f"Erro ao atualizar gráfico: {str(e)}")
    return {"updated": True}


@app.delete("/api/dashboard-charts/{chart_id}")
def delete_chart(chart_id: str, user: User = Depends(get_user)):
    try:
        db.execute(f"DELETE FROM {T_DASH_CHARTS} WHERE chart_id=%(id)s", {"id": chart_id})
    except Exception as e:
        raise HTTPException(400, f"Erro ao excluir gráfico: {str(e)}")
    return {"deleted": True}


@app.post("/api/dashboard-charts/{chart_id}/move")
def move_chart(chart_id: str, body: MoveIn, user: User = Depends(get_user)):
    if body.direction not in ("up", "down"):
        raise HTTPException(400, "direction deve ser 'up' ou 'down'")
    rows = db.query(f"SELECT * FROM {T_DASH_CHARTS} WHERE chart_id=%(id)s", {"id": chart_id})
    if not rows:
        raise HTTPException(404, "Gráfico não encontrado")
    chart = rows[0]
    if body.direction == "up":
        adj = db.query(f"""
            SELECT chart_id, position FROM {T_DASH_CHARTS}
            WHERE view_id=%(vid)s AND position < %(pos)s
            ORDER BY position DESC LIMIT 1
        """, {"vid": chart["view_id"], "pos": chart["position"]})
    else:
        adj = db.query(f"""
            SELECT chart_id, position FROM {T_DASH_CHARTS}
            WHERE view_id=%(vid)s AND position > %(pos)s
            ORDER BY position ASC LIMIT 1
        """, {"vid": chart["view_id"], "pos": chart["position"]})
    if not adj:
        return {"moved": False}
    adj = adj[0]
    now = datetime.utcnow()
    db.execute(
        f"UPDATE {T_DASH_CHARTS} SET position=%(pos)s, updated_at=%(now)s WHERE chart_id=%(id)s",
        {"pos": adj["position"], "now": now, "id": chart_id}
    )
    db.execute(
        f"UPDATE {T_DASH_CHARTS} SET position=%(pos)s, updated_at=%(now)s WHERE chart_id=%(id)s",
        {"pos": chart["position"], "now": now, "id": adj["chart_id"]}
    )
    return {"moved": True}


@app.get("/api/dashboard-charts/{chart_id}/data")
def get_chart_data(chart_id: str, user: User = Depends(get_user)):
    """Execute the chart query and return {labels, datasets, chart_type, palette, title}."""
    try:
        charts = db.query(f"SELECT * FROM {T_DASH_CHARTS} WHERE chart_id=%(id)s", {"id": chart_id})
    except Exception as e:
        raise HTTPException(400, f"Erro ao carregar gráfico: {str(e)}")
    if not charts:
        raise HTTPException(404, "Gráfico não encontrado")

    chart  = charts[0]
    config = json.loads(chart["config"] or "{}")

    test_rows = db.query(f"SELECT * FROM {T_CFG} WHERE test_id=%(id)s", {"id": chart["test_id"]})
    if not test_rows:
        raise HTTPException(404, "Teste do gráfico não encontrado")
    test = test_rows[0]

    chart_type    = chart["chart_type"]
    x_axis        = str(config.get("x_axis") or "ArchiveDate")
    y_aggregation = str(config.get("y_aggregation") or "COUNT").upper()
    y_column      = str(config.get("y_column") or "")
    group_by      = config.get("group_by") or None
    if group_by and str(group_by) in ("", "none", "null"):
        group_by = None
    top_n_series  = int(config.get("top_n_series") or 8)
    date_range    = str(config.get("date_range") or "30d")
    palette       = str(config.get("palette") or "blue")

    table      = _safe_table_name(test["output_table"])
    full_table = f"{CATALOG}.{SCHEMA}.{table}"

    date_filters = {
        "7d":  "ArchiveDate >= date_sub(current_timestamp(), 7)",
        "30d": "ArchiveDate >= date_sub(current_timestamp(), 30)",
        "90d": "ArchiveDate >= date_sub(current_timestamp(), 90)",
        "all": "1=1",
    }
    date_filter = date_filters.get(date_range, date_filters["30d"])

    x_is_date = (x_axis == "ArchiveDate")
    x_expr    = "CAST(DATE(ArchiveDate) AS STRING)" if x_is_date else x_axis
    y_expr    = _build_y_expr(y_aggregation, y_column)
    y_label   = (f"{y_aggregation}({y_column})"
                 if y_aggregation != "COUNT" and y_column and y_column not in ("none", "null", "")
                 else "COUNT(*)")

    try:
        # ── KPI ──────────────────────────────────────────────────────────────
        if chart_type == "kpi":
            sql  = f"SELECT {y_expr} AS val FROM {full_table} WHERE {date_filter}"
            rows = db.query(sql)
            raw  = rows[0]["val"] if rows else 0
            val  = float(raw) if raw is not None else 0.0
            return {
                "chart_type": "kpi", "title": chart["title"], "palette": palette,
                "kpi_value":  int(val) if val == int(val) else round(val, 4),
                "kpi_label":  y_label, "labels": [], "datasets": [],
            }

        # ── Multi-series (group_by) ───────────────────────────────────────────
        if group_by:
            sql = f"""
                WITH top_series AS (
                    SELECT CAST({group_by} AS STRING) AS series, {y_expr} AS total_val
                    FROM {full_table}
                    WHERE {date_filter}
                    GROUP BY 1 ORDER BY 2 DESC LIMIT {top_n_series}
                )
                SELECT {x_expr} AS x_val, CAST({group_by} AS STRING) AS series, {y_expr} AS y_val
                FROM {full_table}
                WHERE {date_filter}
                  AND CAST({group_by} AS STRING) IN (SELECT series FROM top_series)
                GROUP BY 1, 2 ORDER BY 1 ASC
            """
            rows     = db.query(sql)
            all_x    = list(dict.fromkeys(str(r["x_val"]) for r in rows if r["x_val"] is not None))
            all_g    = list(dict.fromkeys(str(r["series"]) for r in rows if r["series"] is not None))
            lookup   = {(str(r["x_val"]), str(r["series"])): r["y_val"] for r in rows}
            datasets = [
                {"label": g, "data": [lookup.get((x, g), 0) for x in all_x]}
                for g in all_g
            ]
            return {
                "chart_type": chart_type, "title": chart["title"], "palette": palette,
                "labels": all_x, "datasets": datasets,
            }

        # ── Single series ─────────────────────────────────────────────────────
        order_clause = "1 ASC" if x_is_date else "2 DESC"
        sql = f"""
            SELECT {x_expr} AS x_val, {y_expr} AS y_val
            FROM {full_table}
            WHERE {date_filter}
            GROUP BY 1
            ORDER BY {order_clause}
            LIMIT 200
        """
        rows   = db.query(sql)
        labels = [str(r["x_val"]) if r["x_val"] is not None else "—" for r in rows]
        values = [r["y_val"] if r["y_val"] is not None else 0 for r in rows]
        return {
            "chart_type": chart_type, "title": chart["title"], "palette": palette,
            "labels": labels, "datasets": [{"label": y_label, "data": values}],
        }

    except Exception as e:
        raise HTTPException(400, f"Erro ao executar query do gráfico: {str(e)}")


# ─────────────────────────────────────────────────────────────────────────────
# Serve frontend
# ─────────────────────────────────────────────────────────────────────────────
FRONTEND_INDEX = os.path.join(os.path.dirname(__file__), "frontend", "index.html")

@app.get("/{full_path:path}", include_in_schema=False)
def spa_fallback(full_path: str):
    if os.path.isfile(FRONTEND_INDEX):
        return FileResponse(FRONTEND_INDEX)
    raise HTTPException(404, "Frontend not found")
