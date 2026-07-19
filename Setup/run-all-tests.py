# Databricks notebook source
# MAGIC %md
# MAGIC # 🎯 Continuous Audit V2 — Orquestrador
# MAGIC >
# MAGIC > Lê os testes ativos de `sandbox.grc.tb_test_configurations` e executa cada um.
# MAGIC > O `test_id` é passado para `run_standard_test` para permitir verificação de supressões.

# COMMAND ----------

# MAGIC %run "/Workspace/GRC/Projects/Continuous Audit v2/continuous-audit/Setup/utils"

# COMMAND ----------

from datetime import datetime
import traceback

# COMMAND ----------

CONFIG_TABLE = "sandbox.grc.tb_test_configurations"

active_tests = (
    spark.table(CONFIG_TABLE)
    .filter("status = 'ACTIVE'")
    .collect()
)

print(f"📋 {len(active_tests)} teste(s) ativo(s) em {CONFIG_TABLE}.")

# COMMAND ----------

for test in active_tests:
    test_name    = test["test_name"]
    test_id      = test["test_id"]
    query_type   = test["query_type"].upper()
    query_code   = test["query_code"]
    imports      = test["imports"] or ""
    output_table = test["output_table"]
    description  = test["description"] or ""
    responsible  = test["responsible_area"] or ""
    risco_id     = test["risco_id"] or "N/A"
    threshold    = test["threshold"] if test["threshold"] is not None else 0
    frequency    = test["frequency"] or "DAILY"
    notify       = bool(test["should_activate_channel"]) if "should_activate_channel" in test else True

    if not should_run_today(frequency):
        print(f"⏭️  Skipping '{test_name}' (frequency: {frequency})")
        continue

    print(f"▶️  Running: {test_name} [{query_type}]")

    try:
        if query_type == "SQL":
            df_incidents = execute_sql_test(query_code)
        elif query_type == "PYTHON":
            df_incidents = execute_python_test(imports, query_code)
        else:
            raise ValueError(f"query_type desconhecido: '{query_type}'")

        run_standard_test(
            test_name=test_name,
            output_table=output_table,
            description=description,
            responsible_area=responsible,
            threshold=threshold,
            result_df=df_incidents,
            frequency=frequency,
            risco_id=risco_id,
            should_activate_channel=notify,
            test_id=test_id,          # ← enables suppression check
        )

        print(f"✅ Finished: {test_name}")

    except Exception as e:
        tb = traceback.format_exc()
        print(f"❌ {test_name} failed:\n{tb}")
        try:
            log_execution(
                test_name=test_name,
                description=description,
                responsible_area=responsible,
                risco_id=risco_id,
                frequency=frequency,
                incident_count=0,
                test_result="ERROR",
                exec_time_sec=0.0,
                threshold=threshold,
                error_message=tb,
            )
        except Exception as log_err:
            print(f"⚠️  Failed to log error for '{test_name}': {log_err}")
        continue

# COMMAND ----------

print(f"\n🏁 Orquestrador finalizado: {now_brt().strftime('%Y-%m-%d %H:%M:%S')} (BRT)")
