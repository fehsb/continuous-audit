# Databricks notebook source
# MAGIC %md
# MAGIC # Continuous Audit V2 — Orquestrador
# MAGIC Executa os testes `ACTIVE` de `tb_test_configurations`.

# COMMAND ----------

# Ambiente de PRODUÇÃO — definido ANTES do %run (o utils lê ao carregar).
import os
os.environ["CA_CATALOG"] = "compliance"
os.environ["CA_SCHEMA"]  = "continuous_audit"

# COMMAND ----------

# MAGIC %run "/Workspace/GRC/Repositórios/job-databricks-continuous-audit/databricks/notebooks/shared/utils"

# COMMAND ----------

# MAGIC %run "/Workspace/GRC/Repositórios/job-databricks-continuous-audit/databricks/notebooks/shared/slack_notifier"

# COMMAND ----------

import traceback

# Campos consumidos de tb_test_configurations (schema completo: Setup/setup-tables.sql):
#   test_id (str, obrigatório) · test_name (str, obrigatório)
#   query_type ("SQL" | "PYTHON", obrigatório — outro valor → ERROR logado)
#   query_code (str, obrigatório) · imports (str, só PYTHON, opcional)
#   output_table (str, obrigatório) · threshold (int, default 0)
#   frequency ("DAILY" | "WEEKLY" | "MONTHLY", default DAILY)
#   should_activate_channel (bool, default True)
#   status — apenas 'ACTIVE' é executado
CONFIG_TABLE = f"{CATALOG}.{SCHEMA}.tb_test_configurations"

active_tests = [r.asDict() for r in
                spark.table(CONFIG_TABLE).filter("status = 'ACTIVE'").collect()]
print(f"📋 {len(active_tests)} teste(s) ativo(s) em {CONFIG_TABLE}")

# COMMAND ----------

executed = skipped = errors = 0

for test in active_tests:
    test_name = test["test_name"]
    frequency = test.get("frequency") or "DAILY"

    if not should_run_today(frequency):
        print(f"⏭️  {test_name} ({frequency})")
        skipped += 1
        continue

    query_type = (test.get("query_type") or "").upper()
    threshold  = test["threshold"] if test.get("threshold") is not None else 0
    notify     = test["should_activate_channel"] if test.get("should_activate_channel") is not None else True

    print(f"▶️  Running: {test_name} [{query_type}]")
    try:
        if query_type == "SQL":
            df_incidents = execute_sql_test(test["query_code"])
        elif query_type == "PYTHON":
            df_incidents = execute_python_test(test.get("imports") or "", test["query_code"])
        else:
            raise ValueError(f"query_type desconhecido: '{query_type}'")

        run_standard_test(
            test_name=test_name,
            output_table=test["output_table"],
            description=test.get("description") or "",
            responsible_area=test.get("responsible_area") or "",
            threshold=threshold,
            result_df=df_incidents,
            frequency=frequency,
            risco_id=test.get("risco_id") or "N/A",
            should_activate_channel=bool(notify),
            test_id=test["test_id"],   # habilita verificação de supressão
        )
        executed += 1
        print(f"✅ Finished: {test_name}")

    except Exception:
        errors += 1
        tb = traceback.format_exc()
        print(f"❌ {test_name} failed:\n{tb}")
        try:
            _last = [l for l in tb.strip().splitlines() if l.strip()][-1][:180]
            record_run_event(test_name, "erro", 0, test.get("risco_id"),
                             test.get("responsible_area"), notify=True, error=_last)
        except Exception:
            pass
        try:
            log_execution(
                test_name=test_name,
                description=test.get("description") or "",
                responsible_area=test.get("responsible_area") or "",
                risco_id=test.get("risco_id") or "N/A",
                frequency=frequency,
                incident_count=0,
                test_result="ERROR",
                exec_time_sec=0.0,
                threshold=threshold,
                error_message=tb,
            )
        except Exception as log_err:
            print(f"⚠️  Falha ao logar erro de '{test_name}': {log_err}")

# COMMAND ----------

# Resumo consolidado no Slack — 1 mensagem por rodada, só quando há trigger
# (novo achado / reincidente / erro). Nunca derruba a rodada.
try:
    risk_levels = {}
    try:
        _rl = spark.sql(f"SELECT RiskId, R2InherentLevel FROM {T_RISKS}").collect()
        risk_levels = {r["RiskId"]: r["R2InherentLevel"] for r in _rl if r["RiskId"]}
    except Exception as _e:
        print(f"⚠️  Níveis de risco indisponíveis para o resumo: {_e}")
    notify_run_summary(RUN_EVENTS, risk_levels=risk_levels,
                       app_url=os.getenv("CA_APP_URL") or None)
except Exception as _e:
    print(f"⚠️  Falha ao enviar resumo Slack (rodada não afetada): {_e}")

# COMMAND ----------

print(f"🏁 {now_brt().strftime('%Y-%m-%d %H:%M:%S')} (BRT) — "
      f"executados: {executed} · pulados: {skipped} · erros: {errors}")
