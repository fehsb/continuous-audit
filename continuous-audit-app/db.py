import os
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, date
from decimal import Decimal
from databricks import sql
from databricks.sdk import WorkspaceClient

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "").replace("https://", "").rstrip("/")
WAREHOUSE_ID    = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
HTTP_PATH       = f"/sql/1.0/warehouses/{WAREHOUSE_ID}"


# ─────────────────────────────────────────────────────────────────────────────
# Auth — the WorkspaceClient is built once and reused. The SDK refreshes the
# token internally, so we no longer rebuild the client / re-authenticate on
# every single query (#1).
# ─────────────────────────────────────────────────────────────────────────────
_ws_client = None
_ws_lock   = threading.Lock()


def _client() -> WorkspaceClient:
    global _ws_client
    if _ws_client is None:
        with _ws_lock:
            if _ws_client is None:
                _ws_client = WorkspaceClient()
    return _ws_client


def _get_token() -> str:
    """
    Bearer token via SDK credential chain. In Databricks Apps the SDK resolves
    the Service Principal (M2M OAuth) automatically; in local dev it falls back
    to DATABRICKS_TOKEN. The client is cached (see _client).
    """
    auth_header = _client().config.authenticate().get("Authorization", "")
    return auth_header.replace("Bearer ", "")


def _new_connection():
    return sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=HTTP_PATH,
        access_token=_get_token(),
        _socket_timeout=30,
        # Pin the SQL session to Brasília so current_timestamp()/DATE() agree
        # with the timestamps the app writes via now_brt() (F8).
        session_configuration={"timezone": "America/Sao_Paulo"},
    )


# ─────────────────────────────────────────────────────────────────────────────
# Per-request connection reuse (#1)
# A pure-ASGI middleware (in main.py) sets a holder in this contextvar for the
# duration of an /api request. The first query in that request lazily opens one
# connection and every later query reuses it; the middleware closes it at the
# end. Outside a request scope (e.g. startup migrations) each call opens and
# closes its own connection, exactly like before.
# ─────────────────────────────────────────────────────────────────────────────
_request_db: ContextVar = ContextVar("_request_db", default=None)


@contextmanager
def _conn():
    holder = _request_db.get()
    if holder is not None:
        if holder["conn"] is None:
            holder["conn"] = _new_connection()
        yield holder["conn"]        # reuse — do NOT close here
        return
    conn = _new_connection()
    try:
        yield conn
    finally:
        conn.close()


BRT_OFFSET = "-03:00"  # Brasília — sem horário de verão desde 2019


def _to_jsonable(v):
    """Normaliza um valor do conector para algo JSON-serializável.

    Timestamps: a sessão do warehouse é pinada em America/Sao_Paulo, então o
    valor devolvido é a HORA DE PAREDE de Brasília — mas às vezes rotulada como
    UTC (ou sem fuso). Descartamos o rótulo e declaramos o offset real (-03:00).

    Colunas complexas: o conector (Arrow) devolve ARRAY como numpy.ndarray e
    numéricos como tipos numpy/Decimal — o FastAPI não serializa ndarray e a
    exceção estoura FORA do try/except dos endpoints (500 "Erro inesperado" na
    tela, ex.: coluna DistinctIPs do teste users-with-many-distinct-ips-per-day).
    tolist() cobre ndarray e escalares numpy via duck typing, com recursão para
    os elementos.
    """
    if isinstance(v, datetime):
        return v.replace(tzinfo=None).isoformat(timespec="seconds") + BRT_OFFSET
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", errors="replace")
    if hasattr(v, "tolist"):            # numpy.ndarray e escalares numpy
        return _to_jsonable(v.tolist())
    if isinstance(v, (list, tuple)):
        return [_to_jsonable(x) for x in v]
    if isinstance(v, dict):
        return {k: _to_jsonable(x) for k, x in v.items()}
    return v


def _serialize(row: dict) -> dict:
    return {k: _to_jsonable(v) for k, v in row.items()}


def query(sql_text: str, params: dict = None) -> list[dict]:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_text, params or {})
            if not cur.description:
                return []
            cols = [c[0] for c in cur.description]
            return [_serialize(dict(zip(cols, row))) for row in cur.fetchall()]


def execute(sql_text: str, params: dict = None) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_text, params or {})
