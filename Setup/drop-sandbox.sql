-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 🗑️ Aposentadoria do sandbox da Auditoria Contínua
-- MAGIC >
-- MAGIC > ⚠️ **DESTRUTIVO.** Dropa **somente** as tabelas da Auditoria Contínua em
-- MAGIC > `sandbox.grc`. O schema é **compartilhado** com outros projetos (nexus, kn1,
-- MAGIC > orion, grafos, KYC, ...) — por isso **não** há DROP SCHEMA aqui, e a seleção
-- MAGIC > é por **lista explícita**, nunca por prefixo genérico.
-- MAGIC >
-- MAGIC > Escopo do drop:
-- MAGIC > - 9 tabelas de controle (`tb_test_*`, `tb_tests_executions`, `tb_incident_hashes`,
-- MAGIC >   `tb_false_positives*`, `tb_dashboard_*`)
-- MAGIC > - Tabelas de achados `tb_incidents_*` (prefixo exclusivo do projeto)
-- MAGIC > - `tb_claude_audit_log_claude_token_audit_log` (achados do teste claude, nome V1)
-- MAGIC >
-- MAGIC > Fora do escopo (ficam intocadas): todo o resto — inclusive `tb_nexus_*`,
-- MAGIC > `tb_kn1_*`, `tb_test_dml_audit_log`, `tb_test_know_your_client`,
-- MAGIC > `tb_user_commands_audit_log*`, ingestões `tb_external_*`, etc.
-- MAGIC >
-- MAGIC > Rode na ordem: 1) prévia → 2) DROP (widget `confirmo=DROP`) → 3) verificação.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Prévia — exatamente o que será dropado (nada é executado aqui)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC SCHEMA_FQ = "sandbox.grc"
-- MAGIC
-- MAGIC # Tabelas de controle da Auditoria Contínua — lista explícita e fechada
-- MAGIC CONTROL_TABLES = [
-- MAGIC     "tb_test_configurations",
-- MAGIC     "tb_test_configurations_history",
-- MAGIC     "tb_tests_executions",
-- MAGIC     "tb_incident_hashes",
-- MAGIC     "tb_test_suppressions",
-- MAGIC     "tb_false_positives",
-- MAGIC     "tb_false_positives_history",
-- MAGIC     "tb_dashboard_views",
-- MAGIC     "tb_dashboard_charts",
-- MAGIC ]
-- MAGIC # Achados com nome fora do padrão tb_incidents_ (herdado do V1)
-- MAGIC EXTRA_INCIDENT_TABLES = [
-- MAGIC     "tb_claude_audit_log_claude_token_audit_log",
-- MAGIC ]
-- MAGIC
-- MAGIC existing = {r["tableName"] for r in spark.sql(f"SHOW TABLES IN {SCHEMA_FQ}").collect()}
-- MAGIC
-- MAGIC to_drop  = sorted(
-- MAGIC     [t for t in CONTROL_TABLES + EXTRA_INCIDENT_TABLES if t in existing]
-- MAGIC     + [t for t in existing if t.startswith("tb_incidents_")]
-- MAGIC )
-- MAGIC keep     = sorted(existing - set(to_drop))
-- MAGIC
-- MAGIC print(f"=== SERÃO DROPADAS ({len(to_drop)}) ===")
-- MAGIC for t in to_drop: print("  🗑️", t)
-- MAGIC print(f"\n=== FICAM INTOCADAS ({len(keep)}) — confira que TODAS as suas estão aqui ===")
-- MAGIC for t in keep: print("  ✅", t)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. DROP
-- MAGIC >
-- MAGIC > Revise a prévia acima. Depois digite **DROP** no widget `confirmo` e rode.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("confirmo", "")
-- MAGIC if dbutils.widgets.get("confirmo") != "DROP":
-- MAGIC     raise RuntimeError("Digite DROP no widget 'confirmo' para executar.")
-- MAGIC
-- MAGIC ok, fail = [], []
-- MAGIC for t in to_drop:
-- MAGIC     try:
-- MAGIC         spark.sql(f"DROP TABLE IF EXISTS {SCHEMA_FQ}.{t}")
-- MAGIC         ok.append(t)
-- MAGIC     except Exception as e:
-- MAGIC         fail.append((t, str(e)[:120]))
-- MAGIC
-- MAGIC print(f"🗑️ {len(ok)} tabela(s) dropada(s).")
-- MAGIC for t, err in fail: print("⚠️ FALHOU:", t, "—", err)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Verificação — nenhuma tabela da Auditoria Contínua deve restar

-- COMMAND ----------

-- MAGIC %python
-- MAGIC existing2 = {r["tableName"] for r in spark.sql(f"SHOW TABLES IN {SCHEMA_FQ}").collect()}
-- MAGIC leftover  = sorted(
-- MAGIC     [t for t in CONTROL_TABLES + EXTRA_INCIDENT_TABLES if t in existing2]
-- MAGIC     + [t for t in existing2 if t.startswith("tb_incidents_")]
-- MAGIC )
-- MAGIC if leftover:
-- MAGIC     print("⚠️ Ainda restam:", leftover)
-- MAGIC else:
-- MAGIC     print(f"✅ Limpo — {len(existing2)} tabela(s) de outros projetos permanecem no schema.")
