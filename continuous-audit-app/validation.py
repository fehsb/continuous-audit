# ─────────────────────────────────────────────────────────────────────────────
# Validation rules — must stay in sync with the frontend localValidate()
# ─────────────────────────────────────────────────────────────────────────────
import re

# SQL is validated with an allow-list (must be a single SELECT/WITH statement),
# which is safer and — unlike the old substring block-list — never blocks legit
# queries that merely mention columns like created_at / updated_at or functions
# like replace().
PYTHON_BLOCKED = [
    "os.system", "subprocess", "__import__",
    "open(", "eval(", "exec(",
    ".write.", "saveastable", "insertinto",
]


def _strip_sql_comments(code: str) -> str:
    """Remove /* block */ and -- line comments so they can't hide a second statement."""
    code = re.sub(r"/\*.*?\*/", " ", code, flags=re.DOTALL)
    code = re.sub(r"--[^\n]*", " ", code)
    return code


def validate(query_type: str, imports: str | None, query_code: str | None) -> None:
    """
    Raises ValueError describing the problem.
    Called on SUBMIT (not on save-as-draft).
    """
    qt   = (query_type or "").upper()
    code = query_code or ""
    imp  = imports or ""

    if qt == "SQL":
        clean = _strip_sql_comments(code).strip()
        if not clean:
            raise ValueError("A query SQL está vazia.")
        # Allow-list: must start with SELECT or WITH (CTE).
        if not re.match(r"^(SELECT|WITH)\b", clean, re.IGNORECASE):
            raise ValueError(
                "Apenas consultas SELECT são permitidas — a query deve começar com SELECT ou WITH."
            )
        # Reject multiple statements: strip a single trailing ';' then look for more.
        body = clean.rstrip().rstrip(";").strip()
        if ";" in body:
            raise ValueError(
                "Múltiplas instruções não são permitidas — use um único SELECT."
            )

    elif qt == "PYTHON":
        full = f"{imp}\n{code}".lower()
        hits = [p for p in PYTHON_BLOCKED if p.lower() in full]
        if hits:
            raise ValueError(
                f"Python contém padrões bloqueados: {hits}. "
                "Escritas diretas, chamadas de sistema e eval/exec não são permitidos."
            )

    else:
        raise ValueError(f"query_type inválido: '{query_type}'. Use SQL ou PYTHON.")
