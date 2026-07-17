-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 📐 Continuous Audit V2 — Setup Consolidado das Tabelas
-- MAGIC >
-- MAGIC > Cria **todas** as tabelas de controle do sistema em qualquer ambiente.
-- MAGIC > Parametrizado por `catalog` / `schema` via widgets — o mesmo notebook serve
-- MAGIC > para o **sandbox** (`sandbox.grc`) e para **produção** (`compliance.continuous_audit`).
-- MAGIC >
-- MAGIC > **Como usar:** ajuste os widgets no topo (ou passe como parâmetros de Job) e rode tudo.
-- MAGIC > Todas as instruções são `CREATE ... IF NOT EXISTS` — rodar de novo não sobrescreve dados.
-- MAGIC >
-- MAGIC > ⚠️ As tabelas `tb_incidents_*` de cada teste são criadas automaticamente no
-- MAGIC > primeiro run do respectivo teste e **não** entram aqui.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Parâmetros do ambiente — sobrescreva ao rodar como Job (base_parameters) ou edite aqui.
-- MAGIC dbutils.widgets.text("catalog", "sandbox")
-- MAGIC dbutils.widgets.text("schema",  "grc")
-- MAGIC print(f"Alvo: {dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Schema

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema}
COMMENT 'Continuous Audit V2';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. `tb_test_configurations` — 1 linha por teste (config + query + status)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.tb_test_configurations (
    test_id           STRING        NOT NULL   COMMENT 'UUID gerado na criação',
    test_name         STRING        NOT NULL   COMMENT 'Nome único do teste em kebab-case',
    output_table      STRING        NOT NULL   COMMENT 'Nome da tabela de incidências (sem schema, começa com tb_incidents_)',
    description       STRING                   COMMENT 'O que o teste verifica',
    responsible_area  STRING                   COMMENT 'Área responsável por acompanhar os resultados',
    risco_id          STRING                   COMMENT 'ID do risco na taxonomia GRC (ex: R271)',
    threshold         INT           NOT NULL   COMMENT 'Limite de incidentes antes de FAILED',
    frequency         STRING        NOT NULL   COMMENT 'DAILY | WEEKLY | MONTHLY',
    query_type        STRING        NOT NULL   COMMENT 'SQL | PYTHON',
    imports           STRING                   COMMENT 'Imports adicionais para testes PYTHON (opcional)',
    query_code        STRING        NOT NULL   COMMENT 'Corpo do teste (SQL SELECT ou Python)',
    status            STRING        NOT NULL   COMMENT 'DRAFT | UNDER_REVIEW | ACTIVE | PAUSED | CANCELLED | REJECTED | PENDING_DELETE',
    category          STRING                   COMMENT 'Categoria livre (ex: cadastro, databricks-config)',
    created_by        STRING        NOT NULL   COMMENT 'Usuário que criou o teste',
    reviewed_by       STRING                   COMMENT 'Usuário que aprovou ou rejeitou',
    rejection_reason  STRING                   COMMENT 'Motivo da rejeição (se REJECTED)',
    created_at        TIMESTAMP     NOT NULL,
    updated_at        TIMESTAMP     NOT NULL,
    activated_at      TIMESTAMP                COMMENT 'Momento em que passou a ACTIVE',
    version           INT           NOT NULL   COMMENT 'Incrementado a cada edição',
    should_activate_channel BOOLEAN            COMMENT 'Se true, dispara notificação quando FAILED'
)
USING DELTA
COMMENT 'Configurações dos testes de auditoria contínua (V2)';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. `tb_test_configurations_history` — histórico imutável de alterações

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.tb_test_configurations_history (
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
-- MAGIC ## 4. `tb_tests_executions` — log de cada execução
-- MAGIC >
-- MAGIC > Inclui os campos do alerta inteligente (`IsSupressed`, `IsRecurrent`, `IsContinued`)
-- MAGIC > e a contagem bruta (`IncidentCountRaw`, antes de descontar falsos positivos).

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.tb_tests_executions (
    TestName              STRING,
    Description           STRING,
    ResponsibleArea       STRING,
    RiskId                STRING,
    Frequency             STRING,
    ExecutionDate         TIMESTAMP,
    IncidentCount         INT           COMMENT 'Contagem limpa (exclui falsos positivos) — usada no threshold',
    IncidentCountRaw      INT           COMMENT 'Contagem bruta antes de descontar FPs',
    ExecutionTimeSec      FLOAT,
    Threshold             INT,
    TestResult            STRING        COMMENT 'PASSED | FAILED | ERROR',
    ShouldActivateChannel BOOLEAN,
    ErrorMessage          STRING        COMMENT 'Traceback quando ERROR; NULL nos demais casos',
    IsSupressed           BOOLEAN       COMMENT 'FAILED silenciado por apontamento de risco aberto',
    IsRecurrent           BOOLEAN       COMMENT 'Mesmos achados voltaram após período limpo',
    IsContinued           BOOLEAN       COMMENT 'Mesmos achados da execução anterior (não realerta)'
)
USING DELTA
COMMENT 'Log de execuções dos testes de auditoria contínua (V2)';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. `tb_incident_hashes` — SHA256 do conjunto de achados por execução (deduplicação)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.tb_incident_hashes (
    test_name         STRING,
    execution_date    TIMESTAMP,
    incident_hash     STRING        COMMENT 'SHA256 do conjunto de achados (exceto ArchiveDate)',
    row_count         INT,
    is_suppressed     BOOLEAN,
    is_recurrent      BOOLEAN
)
USING DELTA
COMMENT 'Hashes de achados por execução — usado para detectar continuidade/reincidência';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. `tb_test_suppressions` — apontamentos de risco que silenciam alertas

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.tb_test_suppressions (
    suppression_id      STRING        NOT NULL   COMMENT 'UUID da supressão',
    test_id             STRING        NOT NULL   COMMENT 'FK para tb_test_configurations.test_id',
    test_name           STRING,
    linked_entry_id     STRING                   COMMENT 'RiskEntryId do apontamento vinculado',
    linked_entry_title  STRING,
    note                STRING,
    created_by          STRING,
    created_at          TIMESTAMP,
    active              BOOLEAN                  COMMENT 'Supressão ativa (true) ou revogada (false)'
)
USING DELTA
COMMENT 'Vincula apontamentos de risco abertos que silenciam alertas de um teste';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. `tb_false_positives` — regras de falso positivo (por critérios)
-- MAGIC >
-- MAGIC > `match_criteria` guarda de 1 a 3 pares `{column, value}` em JSON. Uma linha é FP
-- MAGIC > quando satisfaz TODOS os critérios. `row_hash` fica para compatibilidade com FPs legados.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.tb_false_positives (
    fp_id             STRING        NOT NULL   COMMENT 'UUID do falso positivo',
    test_name         STRING,
    row_hash          STRING                   COMMENT 'Hash da linha de origem (referência / FP legado)',
    match_criteria    STRING                   COMMENT 'JSON [{"column","value"}, ...] — 1 a 3 critérios (match ALL)',
    marked_by         STRING,
    marked_at         TIMESTAMP,
    note              STRING                   COMMENT 'Justificativa obrigatória',
    active            BOOLEAN                  COMMENT 'FP ativo (true) ou removido (false)'
)
USING DELTA
COMMENT 'Regras de falso positivo por critérios de coluna';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8. `tb_false_positives_history` — trilha de marcações/remoções de FP

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.tb_false_positives_history (
    fp_history_id     STRING        NOT NULL,
    fp_id             STRING,
    event_type        STRING                   COMMENT 'marked | unmarked',
    test_name         STRING,
    row_hash          STRING,
    row_data          STRING                   COMMENT 'JSON com as colunas de negócio da linha de origem',
    note              STRING,
    actor             STRING,
    event_at          TIMESTAMP
)
USING DELTA
COMMENT 'Histórico de eventos de falso positivo';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 9. `tb_dashboard_views` — páginas/abas de dashboard customizadas

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.tb_dashboard_views (
    view_id           STRING        NOT NULL,
    title             STRING,
    position          INT,
    created_by        STRING,
    created_at        TIMESTAMP,
    updated_at        TIMESTAMP
)
USING DELTA
COMMENT 'Páginas de dashboard customizadas (até 20)';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 10. `tb_dashboard_charts` — gráficos (widgets) dentro de cada view

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.tb_dashboard_charts (
    chart_id          STRING        NOT NULL,
    view_id           STRING        NOT NULL   COMMENT 'FK para tb_dashboard_views.view_id',
    title             STRING,
    chart_type        STRING                   COMMENT 'line | bar | bar_h | pie | kpi',
    test_id           STRING                   COMMENT 'Fonte de dados (FK para tb_test_configurations.test_id)',
    config            STRING                   COMMENT 'JSON: x_axis, y_aggregation, y_column, group_by, top_n_series, date_range, palette',
    position          INT,
    width             STRING                   COMMENT 'half | full',
    created_at        TIMESTAMP,
    updated_at        TIMESTAMP
)
USING DELTA
COMMENT 'Gráficos configuráveis dentro de uma view de dashboard (até 16 por view)';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 11. Verificação

-- COMMAND ----------

SHOW TABLES IN ${catalog}.${schema};

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > ✅ Devem aparecer as 9 tabelas de controle. As `tb_incidents_*` surgem sozinhas
-- MAGIC > conforme os testes rodam.
