# Databricks notebook source
# MAGIC %md
# MAGIC # Continuous Audit — Orquestrador
# MAGIC Executa os testes `ACTIVE` de `tb_test_configurations` e, ao final,
# MAGIC envia as notificações consolidadas da rodada (resumo no Slack e
# MAGIC cards no Planner, apenas quando há triggers).

# COMMAND ----------

# Ambiente de PRODUÇÃO — definido ANTES do %run (o utils lê ao carregar).
import os
os.environ["CA_CATALOG"] = "compliance"
os.environ["CA_SCHEMA"]  = "continuous_audit"

# COMMAND ----------

# MAGIC %run ../shared/utils

# COMMAND ----------

# MAGIC %run ../shared/slack_notifier

# COMMAND ----------

# MAGIC %run ../shared/planner_notifier

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
    test_name  = test.get("test_name") or f"<sem nome: {test.get('test_id')}>"
    frequency  = test.get("frequency") or "DAILY"
    query_type = (test.get("query_type") or "").upper()
    threshold  = test["threshold"] if test.get("threshold") is not None else 0
    notify     = test["should_activate_channel"] if test.get("should_activate_channel") is not None else True

    # A checagem de agenda é do TESTE, não da rodada: uma frequência inválida
    # (gravada via API/SQL, onde o campo é texto livre) vira erro deste teste em
    # vez de abortar o laço e impedir que as notificações sequer rodem.
    try:
        roda_hoje = should_run_today(frequency)
    except Exception as freq_err:
        errors += 1
        print(f"❌ {test_name}: {freq_err}")
        try:
            record_run_event(test_name, "erro", 0, test.get("risco_id"),
                             test.get("responsible_area"), notify=True,
                             error=freq_err, description=test.get("description"))
            log_execution(
                test_name=test_name,
                description=test.get("description") or "",
                responsible_area=test.get("responsible_area") or "",
                risco_id=test.get("risco_id") or "N/A",
                frequency="DAILY",
                incident_count=0,
                test_result="ERROR",
                exec_time_sec=0.0,
                threshold=threshold,
                error_message=str(freq_err),
            )
        except Exception as log_err:
            print(f"⚠️  Falha ao logar frequência inválida de '{test_name}': {log_err}")
        continue

    if not roda_hoje:
        print(f"⏭️  {test_name} ({frequency})")
        skipped += 1
        continue

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
                             test.get("responsible_area"), notify=True, error=_last,
                             description=test.get("description"))
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

# Notificações da rodada — Slack (resumo) e Planner (cards com dedup).
# Cada canal em try/except próprio: nunca derrubam a rodada nem um ao outro.
_APP_URL = os.getenv("CA_APP_URL", "https://continuous-audit-4061355422303323.gcp.databricksapps.com/")

risk_levels, risk_info = {}, {}
try:
    _rl = spark.sql(f"SELECT RiskId, RiskTitle, R2InherentLevel FROM {T_RISKS}").collect()
    risk_levels = {r["RiskId"]: r["R2InherentLevel"] for r in _rl if r["RiskId"]}
    risk_info   = {r["RiskId"]: {"title": r["RiskTitle"], "level": r["R2InherentLevel"]}
                   for r in _rl if r["RiskId"]}
except Exception as _e:
    print(f"⚠️  Dados de risco indisponíveis para as notificações: {_e}")

_slack_ok = False
try:
    _slack_ok = bool(notify_run_summary(RUN_EVENTS, risk_levels=risk_levels, app_url=_APP_URL))
except Exception as _e:
    print(f"⚠️  Falha ao enviar resumo Slack (rodada não afetada): {_e}")

try:
    notify_planner_cards(RUN_EVENTS, risk_info=risk_info, app_url=_APP_URL)
except Exception as _e:
    print(f"⚠️  Falha nos cards do Planner (rodada não afetada): {_e}")
    # Fase inteira caiu: nenhum card desta rodada foi escrito.
    for _t in list(PENDING_HASHES):
        record_notify_failure(_t)

# Só agora o "já avisamos sobre isso" é consolidado. Se o resumo não saiu, os
# hashes são descartados e a próxima rodada anuncia os achados de novo — repetir
# é aceitável, perder o alerta não é.
if _slack_ok:
    print(f"✅ {flush_pending_hashes()} hash(es) consolidado(s) após a notificação.")
else:
    discard_pending_hashes("o resumo da rodada não foi enviado ao Slack")

# COMMAND ----------

print(f"🏁 {now_brt().strftime('%Y-%m-%d %H:%M:%S')} (BRT) — "
      f"executados: {executed} · pulados: {skipped} · erros: {errors}")
