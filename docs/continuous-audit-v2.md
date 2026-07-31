# Continuous Audit V2 — Documentação de Manutenção

> **Público:** time GRC & Auditoria e qualquer pessoa que precise operar, evoluir ou depurar o sistema.
> **Última revisão:** 31/07/2026

---

## 1. O que é

A **Continuous Audit V2** é o sistema de auditoria contínua automatizada do GRC da CERC. Testes de auditoria (SQL ou PySpark) rodam diariamente no Databricks, detectam riscos materializados ("achados"), classificam a situação de cada teste (novo achado, persistente, reincidente, em tratamento…) e notificam o time por **Slack** (resumo consolidado da rodada) e **Planner** (um card acionável por trigger).

A gestão dos testes é feita por uma **interface web** (Databricks App) — criação, revisão/aprovação, análise de achados, falsos positivos, supressões e dashboards — sem necessidade de editar notebooks.

- **App:** https://continuous-audit-4061355422303323.gcp.databricksapps.com/
- **Ambiente de dados (produção):** `compliance.continuous_audit`

---

## 2. Arquitetura e repositórios

```
┌─────────────────────────────┐        ┌──────────────────────────────────┐
│  APP (Databricks App)       │        │  JOB (Databricks Workflow)       │
│  repo GitHub:               │        │  repo Azure DevOps:              │
│  continuous-audit           │        │  job-databricks-continuous-audit │
│                             │        │                                  │
│  continuous-audit-app/      │  lê/   │  databricks/notebooks/           │
│   main.py   (API FastAPI)   │ grava  │   orchestrator/run-all-tests     │
│   db.py     (SQL Warehouse) │◄──────►│   shared/utils                   │
│   validation.py             │ mesmas │   shared/slack_notifier          │
│   frontend/index.html       │ tabelas│   shared/planner_notifier        │
└─────────────────────────────┘        └───────────┬──────────────────────┘
                                                   │ notificações
                                          ┌────────┴────────┐
                                          │ Slack (webhook) │
                                          │ Planner (Graph) │
                                          └─────────────────┘
```

### 2.1 Divisão de responsabilidade entre repositórios

| Repositório | Onde | Dono de quê | Fluxo de mudança |
|---|---|---|---|
| `continuous-audit` | GitHub | **O app** (`continuous-audit-app/`) | Commit direto na `main` |
| `job-databricks-continuous-audit` | Azure DevOps | **Os jobs** (orquestrador, utils, notifiers, workflows/Terraform) | Branch `hotfix/<nome>` → PR → `main` |

**Regras importantes:**
- O CI do repo do job (`pipelines/ci.yaml`) dispara em push para `main`/`release`/`develop` com mudanças em `databricks/*`. **Branches `hotfix/*` não disparam o CI** — por isso todo trabalho deve nascer nelas. `pr: none` (o pipeline não roda em PR, só no merge).
- Após o merge, fazer **Pull no Git folder do workspace**: `/Workspace/GRC/Repositórios/job-databricks-continuous-audit` — é de lá que o job executa.
- As pastas `Setup/` e `Shared/` do repo do app são **cópias de desenvolvimento/referência** do motor — o deploy real é o repo do job. Ao alterar o motor, a mudança **precisa** chegar ao repo do job.
- A pasta `databricks/notebooks/automated-tests/` no repo do job é **legado do V1** (um notebook por teste). O orquestrador V2 **não usa** esses notebooks — os testes vivem na tabela de configuração.

### 2.2 Ambientes

Tudo é parametrizado por variáveis de ambiente (mesma convenção no app e no job):

| Variável | Default | Uso |
|---|---|---|
| `CA_CATALOG` / `CA_SCHEMA` | `compliance` / `continuous_audit` | Catálogo/schema das tabelas do sistema |
| `DATABRICKS_WAREHOUSE_ID` | (app.yaml) | SQL Warehouse usado pelo app |
| `COMPLIANCE_RISKS_TABLE` | `compliance.sharepoint_list.tb_risks` | Taxonomia de riscos |
| `COMPLIANCE_ENTRIES_TABLE` | `compliance.sharepoint_list.tb_risk_entries` | Apontamentos |
| `COMPLIANCE_AREAS_TABLE` | `compliance.sharepoint_list.tb_areas` | Áreas |
| `RISK_LEVEL_COLUMN` | `R2InherentLevel` | Coluna do nível de risco inerente na tb_risks |
| `CA_APP_URL` | URL do app | Link "Abrir painel" nas notificações |

> O ambiente **sandbox (`sandbox.grc`) foi aposentado** — não recriar.

---

## 3. Modelo de dados (`compliance.continuous_audit`)

| Tabela | Conteúdo |
|---|---|
| `tb_test_configurations` | 1 linha por teste: config + query + status. Inclui colunas `pending_*` (proposta de edição aguardando revisão) |
| `tb_test_configurations_history` | Histórico **imutável** de toda mudança (quem, quando, o quê) — nunca sofre UPDATE/DELETE |
| `tb_tests_executions` | Log de cada execução: `TestResult` (PASSED/FAILED/ERROR), `IncidentCount` (**líquido de falsos positivos** — decide o threshold), `IncidentCountRaw` (bruto), flags `IsSupressed`/`IsRecurrent`/`IsContinued`, `ErrorMessage` |
| `tb_incident_hashes` | SHA-256 do conjunto de achados por execução — base da detecção persistente × reincidente |
| `tb_test_suppressions` | Vínculos teste ↔ apontamento (GRCAP-…) que silenciam alertas |
| `tb_false_positives` | Falsos positivos por **critérios** (`match_criteria`: JSON com 1–3 pares coluna=valor; `row_hash` é legado) |
| `tb_false_positives_history` | Trilha de marcação/remoção de FP |
| `tb_notification_log` | Cards criados no Planner (base do dedup) |
| `tb_dashboard_views` / `tb_dashboard_charts` | Views e gráficos customizados do dashboard |
| `tb_incidents_*` | Uma tabela por teste com os achados (criada automaticamente no 1º run; nome definido na config) |

**Referências externas (sincronizadas do SharePoint):** `tb_risks` (RiskId, RiskTitle, **R2InherentLevel**), `tb_risk_entries` (apontamentos — aberto = `ClosingDate` nulo/vazio), `tb_areas`.

---

## 4. Ciclo de vida de um teste

```
DRAFT ──submeter──► UNDER_REVIEW ──aprovar──► ACTIVE ⇄ PAUSED
  ▲                     │                        │
  └──── (editar) ◄──rejeitar (REJECTED)          │ pedir exclusão
                                                 ▼
                                    PENDING_DELETE ──aprovar──► CANCELLED
                                                 └──rejeitar──► volta a ACTIVE
```

Regras de governança (aplicadas pela API do app):
- **Editar um teste ACTIVE ou PAUSED nunca altera o que roda**: a mudança vai para as colunas `pending_*` e entra na fila de revisão. A versão aprovada continua valendo até outro revisor aprovar (aí `version` incrementa e a proposta é promovida). O revisor vê o **diff da query** e pode rodar o preview da versão proposta.
- **Exclusão tem precedência** sobre edição pendente: aprovar a exclusão cancela o teste e descarta a proposta.
- Ninguém sobrescreve proposta pendente **de outro autor** (HTTP 409); o próprio autor pode atualizar a sua.
- **Auto-aprovação bloqueada** (criador ≠ revisor), exceto e-mails em `SELF_REVIEW_ALLOWED` (`main.py`).
- `test_name` e `output_table` são **únicos** entre testes não-cancelados e ficam **travados** após aprovação.
- Validação de query no submit: SQL usa **allow-list** (uma única instrução iniciando em `SELECT`/`WITH`); Python bloqueia padrões perigosos (`subprocess`, `saveAsTable`, `eval`…) e valida **apenas sintaxe** — a execução real só ocorre no orquestrador.

---

## 5. Motor de alertas (como uma execução é classificada)

Para cada teste ACTIVE cuja frequência cai no dia, o orquestrador executa a query e:

1. Conta os achados **descontando falsos positivos** → compara com o **threshold** → `PASSED` ou `FAILED`.
2. Calcula o **hash** do conjunto de achados e compara com a execução anterior.
3. Verifica se há **supressão ativa** (apontamento vinculado ainda aberto).

| Estado (badge no app) | Condição | Notifica? |
|---|---|---|
| **Sem Achados** | PASSED | — |
| **Novo Achado** | FAILED com conjunto de achados diferente do anterior | ✅ |
| **Achado Persistente** | FAILED com o MESMO hash e a execução anterior já era FAILED | ❌ (evita ruído diário) |
| **Risco Reincidente** | FAILED com o mesmo hash, mas voltou após período limpo | ✅ |
| **Em Tratamento** | FAILED com supressão ativa (apontamento aberto) | ❌ (silenciado até o apontamento fechar) |
| **Erro** | Falha de execução (traceback em `ErrorMessage`) | ✅ (Slack; não vira card) |

**Falsos positivos:** marcados no app por **critérios** (1–3 pares coluna=valor). Uma linha futura que satisfaça TODOS os critérios de algum FP ativo é ignorada na contagem — independente da data. FPs antigos por `row_hash` seguem funcionando como fallback.

**Frequências:** `DAILY` (todo dia), `WEEKLY` (sexta), `MONTHLY` (dia 5) — fronteiras calculadas em **horário de Brasília**.

**Opt-out:** `should_activate_channel = false` no teste desliga Slack/Planner para ele (erros de execução sempre aparecem no Slack).

---

## 6. Fuso horário — regra única

**Tudo é America/Sao_Paulo (BRT, GMT-3, sem horário de verão):**

| Camada | Como |
|---|---|
| Motor (Spark) | Grava datetimes **aware** (com tzinfo) → instante correto independente do TZ do cluster/sessão |
| App (escrita) | `now_brt()` + sessão do warehouse pinada em `America/Sao_Paulo` |
| API | Serializa todo timestamp com offset explícito `-03:00` |
| Frontend | Exibe forçando `America/Sao_Paulo`, de qualquer navegador |

> ⚠️ O cron do workflow (`0 0 6 * * ?`) roda em UTC por padrão = **03:00 BRT**. Se a intenção for 06:00 BRT, ajustar o timezone do schedule no yaml do workflow.

---

## 7. Notificações

### 7.1 Slack — resumo consolidado (`shared/slack_notifier`)

- **Uma única mensagem por rodada**, enviada **apenas quando há trigger** (novo achado, reincidente ou erro). Rodada limpa = silêncio (o card de saúde no app cobre o "rodou?").
- Formato (padrão visual "GRC Alerts", como SISCOM/BC Correio): header com logo, `GRC & Auditoria | data`, linha de overview (executados · sem achados · persistentes · em tratamento), seções **Novos achados / Reincidentes / Erros de execução** com `[Risco · Nível] teste — N achados (área)`, ordenadas por gravidade, e link "Abrir painel".
- Webhook no secret **`compliance-grc/slack-webhook`**. Se o Slack responder `404 no_service`, o webhook foi revogado → gerar um novo em api.slack.com/apps (app "GRC Alerts") e atualizar o secret.

### 7.2 Planner — cards por trigger (`shared/planner_notifier`)

- Para cada **novo achado/reincidente** (erro não vira card): cria um card no plano **Planner_GRC**, bucket **STAND-BY** (sempre), título `[Continuous Audit] {teste}`, prioridade Important, labels rosa+roxa, descrição (o que o teste verifica, risco+nível, área, contagem, data, link) e checklist de 4 passos do fluxo de tratamento.
- **Dedup (regra de ouro):** antes de criar, consulta o último card do teste em `tb_notification_log` e pergunta ao Planner se ainda está **aberto** (`percentComplete < 100`). Aberto → **atualiza** a descrição; concluído/excluído → cria card novo (episódio novo).
- Plano/bucket/labels são resolvidos **por nome** a cada rodada (sobrevive a mudanças de ID).
- Credenciais Graph (client credentials) nos secrets: `planner-tenant-id`, `planner-client-id`, `planner-client-secret` (scope `compliance-grc`). O app registration precisa da permissão de aplicação **`Tasks.ReadWrite.All`** com admin consent.
- ⚠️ Cards criados **manualmente** no board são invisíveis ao dedup (ele rastreia só o que criou, via `tb_notification_log`).

> **Decisão de projeto:** a integração antiga via lista SharePoint (`CONTINUOUS_AUDIT_TRIGGER_DETAILS`) + Power Automate foi **descartada** — não reativar.

---

## 8. O app — mapa rápido para manutenção

| Área | O que tem |
|---|---|
| **Testes** | Faixa de saúde do orquestrador (última rodada/erros/staleness 26h), cards dos 6 estados, lista com risco inerente, tendência (sparkline 14 runs) e alerta atual |
| **Detalhe do teste** | Situação atual, risco & contexto, configuração, governança; abas Query (diff de pendência), Execuções (paginadas, drill p/ achados), Achados (busca no histórico, FP por critérios, export CSV), Em Tratamento (supressões), Histórico (com restore de versão) |
| **Revisão** | Fila de UNDER_REVIEW + PENDING_DELETE + edições pendentes, com diff e preview |
| **Falsos Positivos** | Lista global + histórico de marcações |
| **Dashboard** | KPIs (cobertura atual × execuções no período), gráficos padrão e **views customizadas** (até 20 views × 16 gráficos, montados sobre as tb_incidents_*) |

Notas técnicas do frontend: React 18 via CDN + Babel standalone (**sem build step** — o `index.html` é o app inteiro); tema claro por padrão com toggle escuro; tudo em PT (glossário: Sem achados/Com achados/Erro); preferências persistidas em localStorage.

**Regra de React do projeto:** nunca usar hooks dentro de IIFEs no JSX — todo estado no topo do componente (causa crash silencioso).

**Deploy do app:** commit na `main` do repo GitHub → deploy do Databricks App (`continuous-audit-app/` com `app.yaml`). Migrações de schema leves (colunas `pending_*`, `match_criteria`) rodam automaticamente no startup/uso — o Service Principal precisa de `ALTER` nas tabelas.

---

## 9. Operação — receitas prontas

**Criar um teste:** App → + Novo Teste → preencher (threshold = nº de achados sem FPs acima do qual vira Com Achados) → *Validar e Testar Query* (SQL roda de verdade com LIMIT 10; Python valida só sintaxe) → Submeter → outra pessoa aprova na fila.

**Mexer no motor (utils/orquestrador/notifiers):**
1. Branch `hotfix/<nome>` no repo do job (nunca direto na main — CI).
2. Editar, validar, PR → aprovação → merge na `main` (dispara o CI).
3. **Pull no Git folder do workspace.** Pronto — próxima rodada usa a versão nova.

**Testar notificações sem esperar a rodada:** notebook rascunho no workspace → `%run` do notifier → chamar `notify_run_summary(...)` / `notify_planner_cards(...)` com eventos sintéticos (lista de dicts `{"test_name", "alert", "count", "risco_id", "area", "notify", "error", "description"}`). **Apagar o notebook depois** (nunca deixar webhooks/secrets em células).

**Rotacionar um secret:** gerar novo valor (Slack app / Azure AD) e, num notebook:
```python
from databricks.sdk import WorkspaceClient
WorkspaceClient().secrets.put_secret(scope="compliance-grc", key="<key>", string_value="<novo valor>")
```
Keys em uso: `slack-webhook`, `planner-tenant-id`, `planner-client-id`, `planner-client-secret` (+ legado `sharepoint-*`).

**Rodada manual:** abrir `orchestrator/run-all-tests` no Git folder → Run all. (O app **não** dispara o orquestrador — por definição.)

---

## 10. Troubleshooting

| Sintoma | Causa provável | Ação |
|---|---|---|
| Mensagem não chega no Slack | Rodada sem trigger (comportamento correto) **ou** webhook revogado (`404 no_service` no log do job) | Conferir o log da task; se 404, renovar webhook e atualizar o secret |
| Card duplicado no Planner | Card anterior foi **concluído/excluído** (episódio novo — correto) ou é card **manual/da era V1** (invisível ao dedup) | Concluir/arquivar cards antigos manuais; o dedup cobre os novos |
| Teste em **Erro** | Query quebrou (tabela renomeada, permissão, sintaxe) | Aba Execuções → "Ver erro" (traceback completo) |
| Números do app "descolados" do Slack | Cache de referência (5 min) ou rodada em andamento | Botão Atualizar; conferir `GET /api/orchestrator-health` (`server_now` diagnostica o relógio) |
| Coluna Risco com "—" | Coluna de nível não encontrada na tb_risks | Conferir `RISK_LEVEL_COLUMN` (default `R2InherentLevel`) |
| App lento no 1º acesso do dia | Cold start do SQL Warehouse | O app tenta de novo sozinho (banner); aguardar |
| "Não roda há mais de 26h" na faixa | Job não executou (agenda/cluster) | Conferir o workflow no Databricks; lembrar do fuso do cron |
| Horários estranhos | Regressão de fuso | Ver seção 6 — toda a cadeia é BRT; `server_now` do health ajuda a isolar a camada |

---

## 11. Decisões de projeto registradas

- **SharePoint/Power Automate descartados** para cards — integração direta via Graph.
- **App não dispara o orquestrador** — execução só pelo workflow/manual no Databricks.
- **Slack sem heartbeat** — mensagem só com trigger; o "rodou?" fica na faixa de saúde do app.
- **Persistente não notifica** — anti-ruído por design; reincidente sim.
- **Sandbox aposentado** — ambiente único de produção.
- **Backlog conhecido (não implementado):** papéis de acesso no app (viewer/editor/aprovador), FP com prazo de revalidação, deep-linking (#/tests/:id), acessibilidade (contraste/foco), limpeza dos notebooks legados `automated-tests/`.

---

## 12. Contatos e links

- **App:** https://continuous-audit-4061355422303323.gcp.databricksapps.com/
- **Repo do app (GitHub):** `fehsb/continuous-audit`
- **Repo dos jobs (Azure DevOps):** `Cerc-Recebiveis/Risk-and-Audit-Management` → `job-databricks-continuous-audit`
- **Git folder no workspace:** `/Workspace/GRC/Repositórios/job-databricks-continuous-audit`
- **Canal Slack:** o do app "GRC Alerts" (mesmo dos Alertas SISCOM/BC Correio)
- **Planner:** grupo GRC → plano `Planner_GRC` → bucket `STAND-BY`
