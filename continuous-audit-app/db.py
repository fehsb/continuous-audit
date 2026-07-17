import os
from contextlib import contextmanager
from datetime import datetime
from databricks import sql
from databricks.sdk import WorkspaceClient

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "").replace("https://", "").rstrip("/")
WAREHOUSE_ID    = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
HTTP_PATH       = f"/sql/1.0/warehouses/{WAREHOUSE_ID}"


def _get_token() -> str:
    """
    Obtém o bearer token via SDK credential chain.
    Em Databricks Apps, o SDK resolve automaticamente as credenciais
    da Service Principal via M2M OAuth — sem precisar de DATABRICKS_TOKEN.
    Em dev local, usa DATABRICKS_TOKEN (PAT) como fallback natural do SDK.
    """
    w = WorkspaceClient()
    auth_header = w.config.authenticate().get("Authorization", "")
    return auth_header.replace("Bearer ", "")


@contextmanager
def _conn():
    conn = sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=HTTP_PATH,
        access_token=_get_token(),
        _socket_timeout=30,
    )
    try:
        yield conn
    finally:
        conn.close()


def _serialize(row: dict) -> dict:
    """Convert datetime objects to ISO strings for JSON serialization."""
    return {
        k: (v.isoformat() if isinstance(v, datetime) else v)
        for k, v in row.items()
    }


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
