-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 🗑️ Aposentadoria do sandbox (`sandbox.grc`)
-- MAGIC >
-- MAGIC > ⚠️ **DESTRUTIVO E DEFINITIVO.** Dropa o schema `sandbox.grc` inteiro (todas as
-- MAGIC > tabelas de controle e de achados do ambiente de desenvolvimento).
-- MAGIC >
-- MAGIC > Pré-condições (já garantidas no cutover — commit `da5127a`+):
-- MAGIC > - App em `compliance.continuous_audit` (`app.yaml`)
-- MAGIC > - Job V2 hardcoded em `compliance.continuous_audit`
-- MAGIC > - Defaults de código (utils/app) apontam para produção — nada recria o sandbox
-- MAGIC >
-- MAGIC > **Alvo fixo `sandbox.grc` de propósito** — sem widget de catálogo para não
-- MAGIC > haver risco de apontar para produção por engano.
-- MAGIC >
-- MAGIC > Rode as células na ordem: 1) inventário → 2) trava de segurança → 3) DROP.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Inventário — o que existe hoje em `sandbox.grc`

-- COMMAND ----------

SHOW TABLES IN sandbox.grc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Trava de segurança
-- MAGIC >
-- MAGIC > Confere que **todas** as tabelas do schema têm o prefixo `tb_` do projeto.
-- MAGIC > Se existir qualquer tabela de terceiros, o notebook **aborta** — investigue
-- MAGIC > antes de dropar.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tables = [r["tableName"] for r in spark.sql("SHOW TABLES IN sandbox.grc").collect()]
-- MAGIC alheias = [t for t in tables if not t.startswith("tb_")]
-- MAGIC print(f"{len(tables)} tabela(s) no schema.")
-- MAGIC if alheias:
-- MAGIC     raise RuntimeError(f"ABORTADO: tabelas fora do padrão do projeto: {alheias}")
-- MAGIC print("✅ Todas as tabelas são do projeto (prefixo tb_). Seguro dropar.")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. DROP
-- MAGIC >
-- MAGIC > Digite **DROP** no widget `confirmo` e rode a célula.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("confirmo", "")
-- MAGIC if dbutils.widgets.get("confirmo") != "DROP":
-- MAGIC     raise RuntimeError("Digite DROP no widget 'confirmo' para executar.")
-- MAGIC spark.sql("DROP SCHEMA sandbox.grc CASCADE")
-- MAGIC print("🗑️ sandbox.grc dropado.")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Verificação

-- COMMAND ----------

-- MAGIC %python
-- MAGIC schemas = [r["databaseName"] for r in spark.sql("SHOW SCHEMAS IN sandbox").collect()]
-- MAGIC print("✅ grc removido do catálogo sandbox." if "grc" not in schemas
-- MAGIC       else "⚠️ grc ainda aparece — verifique permissões/erros acima.")
