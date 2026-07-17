-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 📐 Continuous Audit V2 — Criação das Tabelas
-- MAGIC >
-- MAGIC > Este notebook cria as tabelas necessárias para o MVP da **Auditoria Contínua V2** no schema `sandbox.grc`.
-- MAGIC >
-- MAGIC > **Execute uma vez** ao configurar o ambiente. As instruções `CREATE TABLE IF NOT EXISTS` tornam o notebook idempotente — rodar novamente não sobrescreve dados.
-- MAGIC >
-- MAGIC > **Tabelas criadas:**
-- MAGIC > - `sandbox.grc.tb_test_configurations` — configuração e query de cada teste (1 linha por teste)
-- MAGIC > - `sandbox.grc.tb_test_configurations_history` — histórico imutável de alterações
-- MAGIC > - `sandbox.grc.tb_tests_executions` — log de execuções (com coluna `ErrorMessage`)
-- MAGIC >
-- MAGIC > ⚠️ As tabelas `tb_incidents_*` de cada teste são criadas automaticamente no primeiro run do respectivo teste e não precisam ser criadas aqui.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Schema

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS sandbox.grc
COMMENT 'Schema do MVP da Auditoria Contínua V2';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. `tb_test_configurations`
-- MAGIC >
-- MAGIC > Coração do sistema. Uma linha por teste, contendo a configuração completa e a query/código.
-- MAGIC > O novo orquestrador lê esta tabela (filtrando `status = 'ACTIVE'`) para decidir o que executar.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sandbox.grc.tb_test_configurations (
    test_id           STRING        NOT NULL   COMMENT 'UUID gerado na criação',
    test_name         STRING        NOT NULL   COMMENT 'Nome único do teste em kebab-case',
    output_table      STRING        NOT NULL   COMMENT 'Nome da tabela de incidências (sem schema)',
    description       STRING                   COMMENT 'O que o teste verifica',
    responsible_area  STRING                   COMMENT 'Área responsável por acompanhar os resultados',
    risco_id          STRING                   COMMENT 'ID do risco na taxonomia GRC (ex: R271)',
    threshold         INT           NOT NULL   COMMENT 'Limite de incidentes antes de FAILED',
    frequency         STRING        NOT NULL   COMMENT 'DAILY | WEEKLY | MONTHLY',
    query_type        STRING        NOT NULL   COMMENT 'SQL | PYTHON',
    imports           STRING                   COMMENT 'Imports adicionais para testes PYTHON (opcional)',
    query_code        STRING        NOT NULL   COMMENT 'Corpo do teste (SQL SELECT ou Python)',
    status            STRING        NOT NULL   COMMENT 'DRAFT | UNDER_REVIEW | ACTIVE | PAUSED | CANCELLED | REJECTED',
    category          STRING                   COMMENT 'Categoria livre (ex: cadastra, databricks-config)',
    created_by        STRING        NOT NULL   COMMENT 'Usuário que criou o teste',
    reviewed_by       STRING                   COMMENT 'Usuário que aprovou ou rejeitou',
    rejection_reason  STRING                   COMMENT 'Motivo da rejeição (se REJECTED)',
    created_at        TIMESTAMP     NOT NULL,
    updated_at        TIMESTAMP     NOT NULL,
    activated_at      TIMESTAMP                COMMENT 'Momento em que passou a ACTIVE',
    version           INT           NOT NULL   COMMENT 'Incrementado a cada edição aprovada'
)
USING DELTA
COMMENT 'Configurações dos testes de auditoria contínua (V2)';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. `tb_test_configurations_history`
-- MAGIC >
-- MAGIC > Tabela **imutável** — registra todas as mudanças de estado e edições de cada teste.
-- MAGIC > Nunca deve sofrer UPDATE ou DELETE. É a fonte de verdade para auditar quem mudou o quê e quando.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sandbox.grc.tb_test_configurations_history (
    history_id        STRING        NOT NULL   COMMENT 'UUID gerado no evento',
    test_id           STRING        NOT NULL   COMMENT 'FK para tb_test_configurations.test_id',
    test_name         STRING        NOT NULL,
    version           INT           NOT NULL,
    query_type        STRING,
    imports           STRING,
    query_code        STRING,
    status_before     STRING                   COMMENT 'Status anterior ao evento',
    status_after      STRING                   COMMENT 'Status resultante',
    changed_by        STRING        NOT NULL,
    changed_at        TIMESTAMP     NOT NULL,
    change_type       STRING        NOT NULL   COMMENT 'CREATED | EDITED | SUBMITTED_FOR_REVIEW | APPROVED | REJECTED | PAUSED | REACTIVATED | CANCELLED',
    comment           STRING                   COMMENT 'Observação livre (motivo de rejeição etc.)'
)
USING DELTA
COMMENT 'Histórico imutável de alterações nos testes (V2)';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. `tb_tests_executions`
-- MAGIC >
-- MAGIC > Log de execuções dos testes em ambiente sandbox. Equivale à tabela já existente em `compliance.continuous_audit` mas com a coluna nova **`ErrorMessage`** para capturar tracebacks quando `TestResult = 'ERROR'`.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sandbox.grc.tb_tests_executions (
    TestName              STRING,
    Description           STRING,
    ResponsibleArea       STRING,
    RiskId                STRING,
    Frequency             STRING,
    ExecutionDate         TIMESTAMP,
    IncidentCount         INT,
    ExecutionTimeSec      FLOAT,
    Threshold             INT,
    TestResult            STRING        COMMENT 'PASSED | FAILED | ERROR',
    ShouldActivateChannel BOOLEAN,
    ErrorMessage          STRING        COMMENT 'Traceback quando ERROR; NULL nos demais casos'
)
USING DELTA
COMMENT 'Log de execuções dos testes de auditoria contínua (V2)';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Verificação

-- COMMAND ----------

SHOW TABLES IN sandbox.grc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > ✅ Se as três tabelas aparecem acima, o schema está pronto para uso.
-- MAGIC > Próximos passos: deploy do `utils` atualizado e do novo orquestrador.
