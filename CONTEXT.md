# Continuous Audit V2 — Contexto do Projeto

## O que é

Sistema de auditoria contínua automatizada para a equipe GRC da CERC. Testes rodam diariamente no Databricks, detectam riscos materializados e notificam via Slack/SharePoint. Este é o **V2** — reescrita completa com interface web para gerenciar testes sem precisar editar notebooks.

## Arquitetura

```
Databricks Job (06:00 UTC diário)
└── run-all-tests.py  (orquestrador)
    └── Para cada teste ACTIVE em tb_test_configurations:
        ├── Executa a query (SQL ou Python/PySpark)
        ├── Compara hash com execução anterior (deduplicação)
        ├── Verifica supressão (apontamento de risco em aberto)
        └── Grava achados + log + hash

Databricks App (FastAPI + React)
└── main.py     — 30+ endpoints REST
└── db.py       — conexão via SDK ao SQL Warehouse
└── validation.py — bloqueia comandos perigosos
└── frontend/index.html — React via CDN (sem build)
```

## Ambientes

| Env | Catálogo | Schema | Uso |
|-----|----------|--------|-----|
| sandbox | sandbox | grc | desenvolvimento atual |
| produção | compliance | continuous_audit | futuro |

## Tabelas principais (sandbox.grc)

| Tabela | Descrição |
|--------|-----------|
| `tb_test_configurations` | 1 linha por teste — config + query + status |
| `tb_test_configurations_history` | Histórico imutável de cada mudança |
| `tb_tests_executions` | Log de cada execução (PASSED/FAILED/ERROR) |
| `tb_incident_hashes` | SHA256 do conjunto de achados por execução |
| `tb_test_suppressions` | Apontamentos vinculados que silenciam alertas |
| `tb_false_positives` | Linhas marcadas como falso positivo por analistas |
| `tb_incidents_*` | Uma tabela por teste com os achados encontrados |

## Tabelas de referência (compliance.sharepoint_list)

| Tabela | Descrição |
|--------|-----------|
| `tb_risks` | Riscos (RiskId, RiskTitle) |
| `tb_risk_entries` | Apontamentos (RiskEntryId, ClosingDate, etc.) |
| `tb_areas` | Áreas da organização |

Caminhos configuráveis via env vars em `app.yaml`:
- `COMPLIANCE_RISKS_TABLE`
- `COMPLIANCE_AREAS_TABLE`  
- `COMPLIANCE_ENTRIES_TABLE`

## Status de testes (tb_test_configurations.status)

```
DRAFT → UNDER_REVIEW → ACTIVE → PAUSED
                    ↓              ↓
                REJECTED    PENDING_DELETE → CANCELLED
```

- DRAFT/REJECTED podem ser cancelados diretamente (sem aprovação)
- ACTIVE/PAUSED requerem aprovação para exclusão
- Editar um teste ACTIVE bloqueia test_name e output_table

## Status de alerta (última execução)

| Status | Significado |
|--------|-------------|
| `sem_achados` | TestResult=PASSED |
| `novo_achado` | TestResult=FAILED, hash diferente do anterior |
| `persistente` | TestResult=FAILED, mesmo hash, anterior era FAILED (IsContinued=true) |
| `reincidente` | TestResult=FAILED, mesmo hash, anterior era PASSED (IsRecurrent=true) |
| `em_tratamento` | TestResult=FAILED + supressão ativa com entry aberta (IsSupressed=true) |
| `erro` | TestResult=ERROR |

## Lógica de alerta no orquestrador (utils.py)

```python
if base_result == "FAILED":
    suppression = get_active_suppression(test_id)  # verifica ClosingDate IS NULL
    if suppression:
        is_suppressed = True; should_notify = False   # Em Tratamento
    else:
        prev = get_previous_hash(test_name)
        if prev and prev["incident_hash"] == curr_hash:
            if was_previous_result_flagged(test_name):
                is_continued = True; should_notify = False   # Achado Persistente
            else:
                is_recurrent = True; should_notify = True    # Risco Reincidente
        # else: hash diferente → Novo Achado, should_notify = True
```

## Frontend — principais componentes (index.html)

| Componente | Responsabilidade |
|------------|-----------------|
| `App` | Estado global, loadAll(), routing entre páginas |
| `TestList` | Tabela com filtro multi-select de status |
| `TestEditor` | Modal de criação/edição com validação e run preview |
| `TestDetail` | Modal com abas: Info, Query, Execuções, Achados, Em Tratamento, Histórico |
| `AchadosTab` | Preview + banner de hash comparison + botão "Analisar Achados" |
| `AchadosModal` | Modal full com filtros de data, busca, FP por linha |
| `SuppressionTab` | Gerencia apontamentos vinculados (lista e vincula entries abertas) |
| `ReviewQueue` | Fila de aprovação (UNDER_REVIEW + PENDING_DELETE) |
| `Dashboard` | Gráficos — abas "Por Execuções" e "Por Testes" |
| `AlertStatusBadge` | Badge unificado com os 6 status de alerta |
| `RiskCombobox` | Dropdown com busca para selecionar RiskId |
| `AreaSelect` | Dropdown populado de tb_areas |
| `MultiStatusSelect` | Dropdown com checkboxes para filtrar múltiplos status |

## Regras importantes de React neste projeto

**NUNCA use useState/hooks dentro de IIFEs no JSX** — causa crash silencioso.
Todo estado deve estar no topo de um componente funcional.

Exemplo do erro que já aconteceu duas vezes:
```jsx
// ❌ ERRADO — quebra silenciosamente
{(()=>{
  const [tab, setTab] = useState("a");  // PROIBIDO
  return <div>...</div>;
})()}

// ✅ CERTO — estado no topo do componente pai
function MinhaSecao() {
  const [tab, setTab] = useState("a");  // OK
  return <div>...</div>;
}
```

## Convenções de nomenclatura

- `output_table` sempre começa com `tb_incidents_` (prefixo travado no editor)
- `test_name` usa kebab-case, igual ao nome da pasta no repo V1
- `category` segue o padrão do repo V1 (ex: `databricks-config`, `compliance`)

## Endpoints principais (main.py)

```
GET  /api/health              — diagnóstico de conectividade
GET  /api/user                — usuário logado (headers X-Forwarded-Email)
GET  /api/stats               — contagens + by_alert (estado atual)
GET  /api/tests               — lista com last_alert_status
POST /api/tests               — criar teste (?submit=true para submeter)
PUT  /api/tests/{id}          — editar (ACTIVE bloqueia name/table)
POST /api/tests/{id}/approve
POST /api/tests/{id}/reject
POST /api/tests/{id}/pause
POST /api/tests/{id}/activate
POST /api/tests/{id}/request-delete
GET  /api/tests/{id}/executions
GET  /api/tests/{id}/incidents — com _row_hash e _is_false_positive por linha
GET  /api/tests/{id}/suppressions
POST /api/tests/{id}/suppressions
DELETE /api/suppressions/{id}
POST /api/false-positives     — marcar linha como FP
DELETE /api/false-positives/{id}
GET  /api/risks               — tb_risks para combobox
GET  /api/areas               — tb_areas para dropdown
GET  /api/risk-entries        — busca entries abertas para vincular supressão
GET  /api/dashboard           — dados agregados para gráficos
POST /api/run-preview         — testa query (SQL executa real, Python faz parse)
```

## Aprovação e self-review

Configurado em `main.py`:
```python
SELF_REVIEW_ALLOWED = {"fernando.baptista@cerc.com"}
```
Usuários fora desse set não podem aprovar testes que eles mesmos criaram.

## O que ainda não foi feito (backlog)

- [ ] Migrar os ~25 testes do V1 para tb_test_configurations
- [ ] Reativar Slack + SharePoint em utils.py ao ir para produção
- [ ] Trocar CATALOG/SCHEMA de sandbox.grc para compliance.continuous_audit
- [ ] View `vw_incidents_clean` com join automático de false positives
- [ ] Indicador de false positive no histórico de execuções
- [ ] Transformar testes em indicadores/gráficos (mencionado pelo usuário)

## Notas de operação

- `ensure_schema_exists()` em utils.py tem try/except — o SP não tem CREATE SCHEMA mas tem WRITE nas tabelas
- `app.yaml` tem `DATABRICKS_WAREHOUSE_ID` preenchido — nunca sobrescrever com placeholder
- O frontend usa React 18 via CDN + Babel standalone — não há build step
- Chart.js 4.4.0 via CDN para os gráficos do Dashboard
