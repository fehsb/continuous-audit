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
    """Return the SQL with comments removed and string/identifier contents blanked.

    A character-level scan (not regex) so that a ';', '--' or '/*' that lives
    inside a literal — e.g. concat_ws('; ', ...) or LIKE '%--%' — is never
    mistaken for a statement separator or a comment. Quotes are kept so the
    statement shape is preserved; only what is between them becomes spaces.
    Spark rules: '\\' escapes the next char inside '...' and "..."; a doubled
    quote ('' or ``) also escapes.
    """
    out: list[str] = []
    i, n = 0, len(code)
    while i < n:
        c = code[i]
        nxt = code[i + 1] if i + 1 < n else ""
        if c == "-" and nxt == "-":                      # line comment
            j = code.find("\n", i)
            i = n if j < 0 else j
            out.append(" ")
        elif c == "/" and nxt == "*":                    # block comment
            j = code.find("*/", i + 2)
            i = n if j < 0 else j + 2
            out.append(" ")
        elif c in "'\"`":                                # literal / quoted identifier
            q = c
            out.append(q)
            i += 1
            while i < n:
                ch = code[i]
                if ch == "\\" and q != "`" and i + 1 < n:
                    out.append("  ")
                    i += 2
                    continue
                if ch == q:
                    if i + 1 < n and code[i + 1] == q:   # doubled quote = escaped
                        out.append("  ")
                        i += 2
                        continue
                    out.append(q)
                    i += 1
                    break
                out.append("\n" if ch == "\n" else " ")
                i += 1
        else:
            out.append(c)
            i += 1
    return "".join(out)


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
