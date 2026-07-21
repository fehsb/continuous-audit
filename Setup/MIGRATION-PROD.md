# Runbook — Migração para Produção (`compliance.continuous_audit`)

> **STATUS (2026-07-20): MIGRAÇÃO EXECUTADA.** Passos 0–6 concluídos: app e Job V2
> rodando em `compliance.continuous_audit`. A tela **Migração PRD** e os endpoints
> `/api/admin/migration/*` (+ `seed_v1_tests.json`) **já foram removidos do app** —
> as seções abaixo que os citam são registro histórico do procedimento.
> Pendências vivas: ver checklist do Passo 7.

Objetivo: mover o sistema V2 **inteiramente** para `compliance.continuous_audit` e
**desativar o sandbox**. Ao final, nada de operacional roda em `sandbox.grc`.

## Restrições que moldam este plano

1. **Só o app (service principal) e o Job do orquestrador escrevem em PRD.**
   Você não escreve via notebook/SQL editor. Por isso os passos de criação/seed
   rodam pela **tela "Migração PRD"** do app (temporária, visível só para
   `SELF_REVIEW_ALLOWED`), que executa cada passo pelo warehouse com alvo
   **explícito** — independente do `CA_CATALOG` atual do app.
2. **Só UM orquestrador escreve nas `tb_incidents_*`/`tb_tests_executions`.**
   O Job V2 **substitui** o V1 — nunca os dois ativos juntos (senão duplica achados).
3. O app aponta para **um** ambiente por vez (`CA_CATALOG`/`CA_SCHEMA`).

## Decisão de arquitetura — catálogo único

**Config e incidentes ficam juntos em `compliance.continuous_audit`.** Motivos:
todo o código (utils `save_to_table`, app) usa um único par catálogo.schema —
separar exigiria uma segunda env var e mudanças amplas em plena migração; os
prefixos (`tb_test_*`, `tb_false_*`, `tb_dashboard_*` vs `tb_incidents_*`) já
separam logicamente; permissão pode ser dada por tabela. Se a governança exigir,
mover o plano de controle para um schema próprio fica como refactor **pós-migração**.

## Contexto

- `compliance.continuous_audit` **já tem** as 28 `tb_incidents_*` históricas (com
  `ArchiveDate` ✅) e o `tb_tests_executions` do V1.
- Os `output_table` do seed **batem** com esses nomes → **não há cópia de dados**;
  apontar o V2 para lá já traz todo o histórico (dashboards, achados).
- Falta o **plano de controle do V2**: `tb_test_configurations`(+history),
  `tb_incident_hashes`, `tb_test_suppressions`, `tb_false_positives`(+history),
  `tb_dashboard_views/charts`.

---

## O que NÃO PODE falhar (bloqueadores — pare se falhar)

| # | Item | Por quê | Verificação |
|---|---|---|---|
| B1 | **Permissão de escrita do SP do app** em `compliance.continuous_audit` (CREATE TABLE, ALTER, INSERT/DELETE) | Sem isso nenhum passo roda | Passo 0 da tela (probe-write) |

> **B1 — diagnóstico real (2026-07-19):** o probe retornou
> `Catalog 'compliance' has been designated as read-only in current workspace`.
> Não é grant do SP: o catálogo está com **workspace-catalog binding read-only**
> neste workspace — ninguém escreve daqui. Soluções:
> **(A)** admin do metastore muda o binding para Read & Write
> (Catalog Explorer → `compliance` → aba Workspaces → este workspace), ou
> **(B)** deployar o app no workspace onde o Job V1 roda (que já escreve em
> `compliance`) e migrar de lá. Como o app escreve continuamente em produção
> (configs, FPs, dashboards), ele precisa viver num workspace com escrita —
> se este workspace permanecer read-only por governança, (B) é o caminho.
| B2 | **ALTER do `tb_tests_executions` antes de virar o app** | O app faz SELECT de `IsSupressed/IsRecurrent/IsContinued/IncidentCountRaw`; sem as colunas as queries **quebram** (não é só NULL) | Passo 2 da tela; status mostra ✅ por coluna |
| B3 | **Job V1 desativado no momento em que o Job V2 ativa** | Dois orquestradores = achados duplicados no histórico | Manual (projeto do Job) |
| B4 | **Seed com frequências REAIS** (o seed da tela já força isso) | 25 testes DAILY em produção = carga e ruído indevidos | `validate` mostra frequências |
| B5 | **Job V2 com `%run` do utils ATUALIZADO + env `CA_CATALOG/CA_SCHEMA`** | Utils velho = FP por critérios inativo, erro do `reduce`; env errada = escreve no sandbox | Conferir no projeto do Job |

## O que PODE falhar sem drama (retryable / reversível)

| Item | Comportamento em falha |
|---|---|
| Criar tabelas de controle (Passo 1) | `CREATE IF NOT EXISTS` — idempotente; re-executar |
| ALTER executions (Passo 2) | Checa existência antes; re-executar só adiciona o que falta |
| Seed (Passo 3) | Idempotente por `test_name` (DELETE+INSERT); re-executar corrige parciais. **Atenção:** sobrescreve edições manuais feitas nesses 28 testes |
| Virar o app (Passo 5) | Reversível: voltar `CA_CATALOG/CA_SCHEMA` no app.yaml e redeploy |
| Validação (Passo 4) | Read-only |

## Riscos conhecidos (não bloqueiam, mas saiba)

- **Primeira execução V2 em prod = possível rajada de alertas.** `tb_incident_hashes`
  não existe no V1, então todo teste FAILED da 1ª rodada é tratado como "novo achado"
  (`should_notify=true`). Hoje as notificações estão desligadas no utils (`pass`), então
  é inofensivo — **mas reative Slack/SharePoint só DEPOIS da 1ª rodada V2** para não
  notificar achados antigos como novos.
- **Linhas históricas do executions** ficam com NULL nas colunas novas — o app trata.
- **Seed sobrescreve os 28 test_names** — se alguém editou um desses testes pela UI
  em prod antes do seed, a edição é perdida (não re-rode o Passo 3 depois do cutover).

---

## Passo a passo

### Passo 0 — Código (feito)
- `utils.py` e `run-all-tests.py` parametrizados por `CA_CATALOG`/`CA_SCHEMA` (default sandbox/grc).
- App idem (`app.yaml`).
- Tela **Migração PRD** no app + endpoints `/api/admin/migration/*` (alvo explícito, gated).
- `seed_v1_tests.json` empacotado no app (28 testes, frequências reais).

### Passo 1..4 — Pela tela "Migração PRD" (alvo: `compliance.continuous_audit`)

> A tela fica no menu lateral (só para `SELF_REVIEW_ALLOWED`). Execute na ordem;
> cada passo mostra o resultado JSON e atualiza o painel de status.

| Passo na tela | O quê | Falhou? |
|---|---|---|
| **0 · Probe de escrita** | Cria+dropa tabela probe no alvo | **PARE** — resolver permissão do SP (B1) |
| **1 · Criar tabelas de controle** | 9 tabelas, `IF NOT EXISTS` | Re-executar; se persistir, é permissão |
| **2 · ALTER executions** | Colunas novas no `tb_tests_executions` V1 | **Não vire o app sem isso** (B2) |
| **3 · Seed dos 28 testes** | Configs com frequências reais, `created_by=v1-migration` | Re-executar (idempotente) |
| **4 · Validar** | 25 ACTIVE + 3 PAUSED, colunas ok, amostra do histórico com `ArchiveDate` | Investigar item reprovado antes de seguir |

### Passo 5 — Virar a escrita (orquestrador — projeto separado)
- Job V2 com env `CA_CATALOG=compliance`, `CA_SCHEMA=continuous_audit`, `%run` do utils
  atualizado, schedule 06:00 UTC diário.
- **Desativar o Job V1 no mesmo movimento** (B3).
- 1ª execução: apenda achados e loga com as colunas novas.

### Passo 6 — Virar o app
No `app.yaml`: `CA_CATALOG: "compliance"`, `CA_SCHEMA: "continuous_audit"` → redeploy.
**Só depois** dos passos 1–4. Validar na tela: 28 testes, histórico em "Analisar Achados",
dashboards com dados reais.

### Passo 7 — Consolidar e desligar o sandbox
Depois de validado (alguns dias de rodadas limpas):
- [ ] Reativar Slack/SharePoint no utils (**após** a 1ª rodada V2 — ver Riscos).
- [x] Trocar os defaults de `CA_CATALOG`/`CA_SCHEMA` para produção — app.yaml,
      orquestrador, `utils.py` e `main.py` (nada recria o sandbox por engano).
- [x] **Remover a tela Migração PRD** e os endpoints `/api/admin/migration/*` do app (2026-07-20).
- [ ] Dropar `sandbox.grc` — rodar `Setup/drop-sandbox.sql` no workspace de dev
      (inventário → trava de segurança → widget `confirmo=DROP`).

---

## Rollback

- **App:** reverter `app.yaml` para `sandbox`/`grc` + redeploy (minutos).
- **Orquestrador:** reativar o Job V1 (se ainda existir) e desativar o V2 — nunca os dois.
- Tabelas de controle criadas em prod são inertes se app/Job não apontarem para lá.
- O seed não toca nas `tb_incidents_*` — o histórico nunca está em risco neste plano.

## Checklist final

- [ ] Probe de escrita OK em compliance
- [ ] 9 tabelas de controle existem
- [ ] 4 colunas novas no `tb_tests_executions`
- [ ] Configs: 25 ACTIVE + 3 PAUSED, `created_by=v1-migration`, frequências reais
- [ ] Job V2 ativo, Job V1 desativado
- [ ] App em compliance; histórico visível; dashboards com dados
- [ ] 1ª rodada V2 sem ERRORs inesperados
- [ ] Notificações reativadas (após 1ª rodada)
- [ ] Tela de migração removida; sandbox desligado
