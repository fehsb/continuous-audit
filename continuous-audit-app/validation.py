# ─────────────────────────────────────────────────────────────────────────────
# Validation rules — must stay in sync with utils.py (Databricks shared module)
# ─────────────────────────────────────────────────────────────────────────────

SQL_BLOCKED = [
    "DROP", "CREATE", "INSERT", "DELETE",
    "ALTER", "UPDATE", "TRUNCATE", "REPLACE",
    "MERGE", "OVERWRITE",
]

PYTHON_BLOCKED = [
    "os.system", "subprocess", "__import__",
    "open(", "eval(", "exec(",
    ".write.", "saveAsTable", "insertInto",
    "DROP ", "CREATE TABLE", "INSERT INTO",
    "DELETE FROM", "ALTER TABLE", "TRUNCATE TABLE",
]


def validate(query_type: str, imports: str | None, query_code: str | None) -> None:
    """
    Raises ValueError listing all blocked patterns found.
    Called on SUBMIT (not on save-as-draft).
    """
    qt = (query_type or "").upper()
    code = query_code or ""
    imp  = imports or ""

    if qt == "SQL":
        hits = [k for k in SQL_BLOCKED if k in code.upper()]
        if hits:
            raise ValueError(f"SQL contém palavras-chave bloqueadas: {hits}. Apenas SELECT é permitido.")

    elif qt == "PYTHON":
        full = f"{imp}\n{code}".upper()
        hits = [p for p in PYTHON_BLOCKED if p.upper() in full]
        if hits:
            raise ValueError(f"Python contém padrões bloqueados: {hits}. Escritas diretas, chamadas de sistema e eval/exec não são permitidos.")

    else:
        raise ValueError(f"query_type inválido: '{query_type}'. Use SQL ou PYTHON.")
