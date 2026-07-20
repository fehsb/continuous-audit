# Databricks notebook source
# MAGIC %md
# MAGIC # Continuous Audit V2 — Orquestrador
# MAGIC Executa os testes `ACTIVE` de `tb_test_configurations`.
# MAGIC Requer o notebook `utils` **no mesmo diretório**.

# COMMAND ----------

# Ambiente de PRODUÇÃO — definido ANTES do %run (o utils lê ao carregar).
import os
os.environ["CA_CATALOG"] = "compliance"
os.environ["CA_SCHEMA"]  = "continuous_audit"

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

import traceback

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

print(f"🏁 {now_brt().strftime('%Y-%m-%d %H:%M:%S')} (BRT) — "
      f"executados: {executed} · pulados: {skipped} · erros: {errors}")
