# Databricks notebook source
# MAGIC %md
# MAGIC # 🛠️ Continuous Audit V2 — Shared Utils
# MAGIC >
# MAGIC > **Mudanças nesta versão:**
# MAGIC > - `compute_incident_hash` — SHA256 do conjunto de achados (exceto ArchiveDate)
# MAGIC > - `get_active_suppression` — verifica se teste tem supressão ativa com entry aberta
# MAGIC > - `get_previous_hash` / `was_previous_result_flagged` — detecta continuidade vs reincidência
# MAGIC > - `run_standard_test` — lógica completa de alerta inteligente
# MAGIC > - `log_execution` — novos campos `is_suppressed` e `is_recurrent`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

# Slack and SharePoint disabled in sandbox — re-enable via %run when migrating to production.

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from functools import reduce   # disponível no escopo dos testes migrados do V1
import hashlib
import json
import os
import traceback
import textwrap

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

CATALOG   = "sandbox"
SCHEMA    = "grc"
T_ENTRIES = os.getenv("COMPLIANCE_ENTRIES_TABLE", "compliance.sharepoint_list.tb_risk_entries")

# Fuso oficial do sistema: Brasília (F8). A sessão Spark e todos os timestamps
# gravados usam America/Sao_Paulo — o app (main.py/db.py) segue a mesma regra.
from zoneinfo import ZoneInfo
BRT = ZoneInfo("America/Sao_Paulo")

try:
    spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")
except Exception:
    pass  # sem permissão para alterar a conf — segue o TZ do cluster


def now_brt() -> datetime:
    """Agora em Brasília, COM tzinfo (aware).

    Datetime aware é convertido pelo Spark para o instante correto
    independentemente do timezone da sessão ou do cluster — a versão naive
    dependia do spark.sql.session.timeZone estar em BRT; quando a sessão
    ficava em UTC, a parede de Brasília era gravada como UTC (instante 3h
    atrás de verdade), e o app exibia a rodada 3h no passado.
    """
    return datetime.now(BRT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Frequency helpers

# COMMAND ----------

def should_run_today(frequency: str) -> bool:
    today    = now_brt()   # fronteiras de dia/semana/mês seguem Brasília (F8)
    weekday  = today.weekday()
    day      = today.day
    frequency = frequency.upper()
    if frequency == "DAILY":    return True
    elif frequency == "WEEKLY": return weekday == 4
    elif frequency == "MONTHLY":return day == 5
    else: raise ValueError(f"Invalid frequency: {frequency}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persistence helpers

# COMMAND ----------

def ensure_schema_exists() -> None:
    """
    Best-effort schema creation. Silently ignored if the schema already exists
    or if the service principal lacks CREATE SCHEMA — tables can still be written
    as long as the schema was created once by an admin.
    """
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    except Exception:
        pass  # Schema already exists or SP lacks CREATE SCHEMA — both are fine

def save_to_table(df: DataFrame, table_base: str) -> None:
    ensure_schema_exists()
    (df.write.mode("append").format("delta")
       .option("mergeSchema", "true")
       .saveAsTable(f"{CATALOG}.{SCHEMA}.{table_base}"))

# COMMAND ----------

def log_execution(
    test_name: str,
    description: str,
    responsible_area: str,
    risco_id: str,
    frequency: str,
    incident_count: int,
    test_result: str,
    exec_time_sec: float,
    threshold: int,
    should_activate_channel: bool = True,
    error_message: str = None,
    is_suppressed: bool = False,
    is_recurrent:  bool = False,
    is_continued:  bool = False,
    incident_count_raw: int = None,
) -> None:
    schema_def = StructType([
        StructField("TestName",              StringType(),    True),
        StructField("Description",           StringType(),    True),
        StructField("ResponsibleArea",       StringType(),    True),
        StructField("RiskId",                StringType(),    True),
        StructField("Frequency",             StringType(),    True),
        StructField("ExecutionDate",         TimestampType(), True),
        StructField("IncidentCount",         IntegerType(),   True),
        StructField("IncidentCountRaw",      IntegerType(),   True),
        StructField("ExecutionTimeSec",      FloatType(),     True),
        StructField("Threshold",             IntegerType(),   True),
        StructField("TestResult",            StringType(),    True),
        StructField("ShouldActivateChannel", BooleanType(),   True),
        StructField("ErrorMessage",          StringType(),    True),
        StructField("IsSupressed",           BooleanType(),   True),
        StructField("IsRecurrent",           BooleanType(),   True),
        StructField("IsContinued",           BooleanType(),   True),
    ])
    data = [(
        test_name, description, responsible_area, risco_id, frequency,
        now_brt(), incident_count, incident_count_raw, float(exec_time_sec), threshold,
        test_result, should_activate_channel, error_message,
        is_suppressed, is_recurrent, is_continued,
    )]
    df_log = spark.createDataFrame(data, schema=schema_def)
    ensure_schema_exists()
    (df_log.write.mode("append").option("mergeSchema", "true")
           .saveAsTable(f"{CATALOG}.{SCHEMA}.tb_tests_executions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hash helpers

# COMMAND ----------

def compute_incident_hash(df: DataFrame) -> str | None:
    """
    SHA256 determinístico do conjunto de achados, excluindo ArchiveDate.
    Hashes por linha calculados no Spark; só strings pequenas são coletadas.
    """
    if df.count() == 0:
        return None

    hash_cols = sorted([c for c in df.columns if c not in ("ArchiveDate", "_is_false_positive")])

    row_hash_df = df.withColumn(
        "_row_hash",
        sha2(concat_ws("|", *[col(c).cast("string") for c in hash_cols]), 256)
    )

    row_hashes = sorted([r["_row_hash"] for r in row_hash_df.select("_row_hash").collect()])
    return hashlib.sha256("\n".join(row_hashes).encode("utf-8")).hexdigest()


def save_incident_hash(test_name, incident_hash, row_count, is_suppressed, is_recurrent) -> None:
    schema_def = StructType([
        StructField("test_name",      StringType(),    True),
        StructField("execution_date", TimestampType(), True),
        StructField("incident_hash",  StringType(),    True),
        StructField("row_count",      IntegerType(),   True),
        StructField("is_suppressed",  BooleanType(),   True),
        StructField("is_recurrent",   BooleanType(),   True),
    ])
    df_h = spark.createDataFrame(
        [(test_name, now_brt(), incident_hash, row_count, is_suppressed, is_recurrent)],
        schema=schema_def
    )
    ensure_schema_exists()
    (df_h.write.mode("append").option("mergeSchema", "true")
          .saveAsTable(f"{CATALOG}.{SCHEMA}.tb_incident_hashes"))


def add_fp_flags(df: DataFrame, test_name: str) -> DataFrame:
    """
    Adiciona coluna _is_false_positive ao DataFrame antes de persistir.

    Modelo por critérios: cada FP em tb_false_positives define de 1 a 3 pares
    (coluna, valor) em match_criteria (JSON). Uma linha é falso positivo se
    satisfizer TODOS os critérios de algum FP ativo — independente de data ou
    dos demais campos. FPs antigos sem match_criteria continuam funcionando
    por hash da linha inteira (compatibilidade retroativa).
    """
    try:
        fp_rows = spark.sql(f"""
            SELECT row_hash, match_criteria
            FROM {CATALOG}.{SCHEMA}.tb_false_positives
            WHERE test_name = '{test_name}' AND active = true
        """).collect()
    except Exception:
        # Coluna match_criteria pode não existir ainda — cai no schema legado
        try:
            fp_rows = spark.sql(f"""
                SELECT row_hash
                FROM {CATALOG}.{SCHEMA}.tb_false_positives
                WHERE test_name = '{test_name}' AND active = true
            """).collect()
        except Exception:
            fp_rows = []

    available     = set(df.columns)
    criteria_conds = []   # uma Column booleana por FP com critérios
    legacy_hashes  = []   # row_hash de FPs antigos sem critérios

    for r in fp_rows:
        d = r.asDict()
        parsed = None
        raw = d.get("match_criteria")
        if raw:
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = None
        if parsed:
            cond, valid = None, True
            for c in parsed:
                cname = c.get("column")
                cval  = "" if c.get("value") is None else str(c.get("value"))
                if cname not in available:
                    valid = False
                    break
                # coalesce(…, "") espelha str(None) → "" no lado Python
                pred = coalesce(col(cname).cast("string"), lit("")) == lit(cval)
                cond = pred if cond is None else (cond & pred)
            if valid and cond is not None:
                criteria_conds.append(cond)
        elif d.get("row_hash"):
            legacy_hashes.append(d["row_hash"])

    if not criteria_conds and not legacy_hashes:
        return df.withColumn("_is_false_positive", lit(False))

    match_expr = None
    for cond in criteria_conds:
        match_expr = cond if match_expr is None else (match_expr | cond)

    result = df
    if legacy_hashes:
        # Exclude ArchiveDate and internal _-prefixed columns (same rule as Python's _row_hash)
        hash_cols = sorted([c for c in df.columns if c != "ArchiveDate" and not c.startswith("_")])
        result = result.withColumn(
            "_row_hash_tmp",
            sha2(concat_ws("|", *[coalesce(col(c).cast("string"), lit("")) for c in hash_cols]), 256)
        )
        hash_cond  = col("_row_hash_tmp").isin(legacy_hashes)
        match_expr = hash_cond if match_expr is None else (match_expr | hash_cond)

    result = result.withColumn("_is_false_positive", match_expr)
    if "_row_hash_tmp" in result.columns:
        result = result.drop("_row_hash_tmp")
    return result


def get_previous_hash(test_name: str) -> dict | None:
    try:
        rows = spark.sql(f"""
            SELECT incident_hash, row_count, is_suppressed
            FROM {CATALOG}.{SCHEMA}.tb_incident_hashes
            WHERE test_name = '{test_name}'
            ORDER BY execution_date DESC
            LIMIT 1
        """).collect()
        return rows[0].asDict() if rows else None
    except Exception:
        return None


def was_previous_result_flagged(test_name: str) -> bool:
    """True se a execução anterior foi FAILED e não estava suprimida."""
    try:
        rows = spark.sql(f"""
            SELECT TestResult, IsSupressed
            FROM {CATALOG}.{SCHEMA}.tb_tests_executions
            WHERE TestName = '{test_name}'
            ORDER BY ExecutionDate DESC
            LIMIT 1
        """).collect()
        if not rows:
            return False
        r = rows[0].asDict()
        return r.get("TestResult") == "FAILED" and not r.get("IsSupressed", False)
    except Exception:
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Suppression helpers

# COMMAND ----------

def get_active_suppression(test_id: str) -> dict | None:
    """
    Retorna supressão ativa se o teste tiver uma E o apontamento ainda estiver aberto
    (ClosingDate IS NULL ou vazio em tb_risk_entries).
    Falha silenciosamente se T_ENTRIES não estiver acessível.
    """
    if not test_id:
        return None
    try:
        rows = spark.sql(f"""
            SELECT
                s.suppression_id,
                s.linked_entry_id,
                s.linked_entry_title,
                s.note
            FROM {CATALOG}.{SCHEMA}.tb_test_suppressions s
            LEFT JOIN {T_ENTRIES} e ON e.RiskEntryId = s.linked_entry_id
            WHERE s.test_id = '{test_id}'
              AND s.active  = true
              AND (e.ClosingDate IS NULL OR TRIM(COALESCE(e.ClosingDate, '')) = '')
        """).collect()
        return rows[0].asDict() if rows else None
    except Exception as ex:
        print(f"⚠️ Suppression check failed (non-critical): {ex}")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dynamic execution helpers

# COMMAND ----------

def execute_sql_test(query_code: str) -> DataFrame:
    return spark.sql(query_code)


def execute_python_test(imports: str, query_code: str) -> DataFrame:
    local_ctx = dict(globals())
    if imports and imports.strip():
        clean = "\n".join(l.strip() for l in imports.splitlines() if l.strip())
        exec(clean, local_ctx)
    exec(textwrap.dedent(query_code), local_ctx)
    if "df_incidents" not in local_ctx:
        raise ValueError("O código Python deve atribuir o resultado a 'df_incidents'.")
    return local_ctx["df_incidents"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Core runner

# COMMAND ----------

def run_standard_test(
    test_name:               str,
    output_table:            str,
    description:             str,
    responsible_area:        str,
    threshold:               int,
    result_df:               DataFrame,
    frequency:               str  = "DAILY",
    risco_id:                str  = "N/A",
    should_activate_channel: bool = True,
    test_id:                 str  = None,
) -> None:
    if not should_run_today(frequency):
        print(f"⏭️ Skipping '{test_name}' (frequency: {frequency})")
        return

    try:
        start              = now_brt()
        incident_count_raw = result_df.count()

        # Hash calculado sobre todas as linhas brutas (sem ArchiveDate / _is_false_positive)
        curr_hash = compute_incident_hash(result_df) if incident_count_raw > 0 else None

        # Adiciona _is_false_positive e ArchiveDate antes de persistir
        result_df = add_fp_flags(result_df, test_name)
        result_df = result_df.withColumn("ArchiveDate", lit(now_brt()).cast("timestamp"))
        exec_time = (now_brt() - start).total_seconds()

        # Contagem limpa (exclui FPs) determina threshold e alertas
        incident_count = result_df.filter(col("_is_false_positive") == False).count()
        base_result    = "FAILED" if incident_count > threshold else "PASSED"

        is_suppressed = False
        is_recurrent  = False
        is_continued  = False
        should_notify = should_activate_channel and base_result == "FAILED"

        # ── Lógica de alerta inteligente ──────────────────────────────────────
        if base_result == "FAILED":

            # 1. Supressão: apontamento aberto associado ao teste?
            suppression = get_active_suppression(test_id)
            if suppression:
                is_suppressed = True
                should_notify = False
                print(f"🔇 '{test_name}' suprimido — apontamento {suppression['linked_entry_id']} em aberto")

            else:
                prev = get_previous_hash(test_name)

                if prev and prev.get("incident_hash") == curr_hash:
                    # Mesmo conjunto de achados que na execução anterior
                    if was_previous_result_flagged(test_name):
                        # Problema contínuo sem resolução — NÃO alerta de novo
                        is_continued  = True
                        should_notify = False
                        print(f"🔄 '{test_name}' continuidade — mesmos achados, não repetindo alerta")
                    else:
                        # Voltou após período Cleared ou suprimido — REINCIDÊNCIA
                        is_recurrent  = True
                        should_notify = should_activate_channel
                        print(f"⚠️  '{test_name}' REINCIDÊNCIA — achados idênticos após período limpo")
                else:
                    # Achados diferentes (novos ou parcialmente resolvidos) — alerta
                    print(f"🆕 '{test_name}' achados {'diferentes' if prev else 'primeira execução'}")

        # ── Persistência ──────────────────────────────────────────────────────
        save_to_table(result_df, output_table)
        save_incident_hash(test_name, curr_hash, incident_count_raw, is_suppressed, is_recurrent)
        log_execution(
            test_name=test_name,
            description=description,
            responsible_area=responsible_area,
            risco_id=risco_id,
            frequency=frequency,
            incident_count=incident_count,
            incident_count_raw=incident_count_raw,
            test_result=base_result,
            exec_time_sec=exec_time,
            threshold=threshold,
            should_activate_channel=should_activate_channel,
            error_message=None,
            is_suppressed=is_suppressed,
            is_recurrent=is_recurrent,
            is_continued=is_continued,
        )

        # ── Notificação (reativar em produção) ────────────────────────────────
        if should_notify:
            pass  # notify_slack_incident(...) / update_sharepoint_trigger(...)

    except Exception as e:
        log_execution(
            test_name=test_name,
            description=description,
            responsible_area=responsible_area,
            risco_id=risco_id,
            frequency=frequency,
            incident_count=0,
            test_result="ERROR",
            exec_time_sec=0,
            threshold=threshold,
            should_activate_channel=should_activate_channel,
            error_message=traceback.format_exc(),
        )
        raise e

