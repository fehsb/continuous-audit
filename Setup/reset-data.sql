-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 🧹 Continuous Audit V2 — Limpeza de Dados
-- MAGIC >
-- MAGIC > ⚠️ **DESTRUTIVO.** Apaga os dados das tabelas de controle para começar do zero
-- MAGIC > (ex.: antes de replicar os testes do V1 no sandbox). Rode conscientemente.
-- MAGIC >
-- MAGIC > Por padrão usa `TRUNCATE` (esvazia, mantém o schema). As tabelas `tb_incidents_*`
-- MAGIC > de cada teste são descartadas com `DROP` na última célula.
-- MAGIC >
-- MAGIC > Parametrizado por `catalog` / `schema` — **confirme que aponta para o ambiente certo.**

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog", "sandbox")
-- MAGIC dbutils.widgets.text("schema",  "grc")
-- MAGIC alvo = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"
-- MAGIC print(f"⚠️  Vai limpar TODOS os dados de: {alvo}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Esvaziar tabelas de controle

-- COMMAND ----------

TRUNCATE TABLE ${catalog}.${schema}.tb_test_configurations;
TRUNCATE TABLE ${catalog}.${schema}.tb_test_configurations_history;
TRUNCATE TABLE ${catalog}.${schema}.tb_tests_executions;
TRUNCATE TABLE ${catalog}.${schema}.tb_incident_hashes;
TRUNCATE TABLE ${catalog}.${schema}.tb_test_suppressions;
TRUNCATE TABLE ${catalog}.${schema}.tb_false_positives;
TRUNCATE TABLE ${catalog}.${schema}.tb_false_positives_history;
TRUNCATE TABLE ${catalog}.${schema}.tb_dashboard_views;
TRUNCATE TABLE ${catalog}.${schema}.tb_dashboard_charts;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Descartar as tabelas de achados (`tb_incidents_*`)
-- MAGIC >
-- MAGIC > Elas são recriadas sozinhas no próximo run de cada teste.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC catalog = dbutils.widgets.get("catalog")
-- MAGIC schema  = dbutils.widgets.get("schema")
-- MAGIC tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
-- MAGIC incident_tables = [r["tableName"] for r in tables if r["tableName"].startswith("tb_incidents_")]
-- MAGIC print(f"Encontradas {len(incident_tables)} tabela(s) de achados:")
-- MAGIC for t in incident_tables:
-- MAGIC     print("  -", t)
-- MAGIC # Descomente para efetivar o DROP:
-- MAGIC # for t in incident_tables:
-- MAGIC #     spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{t}")
-- MAGIC #     print("DROP", t)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > ✅ Ambiente limpo. Rode `setup-tables.sql` se precisar recriar o schema e, em seguida,
-- MAGIC > o seed dos testes do V1.
