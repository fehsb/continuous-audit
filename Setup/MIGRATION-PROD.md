# Runbook — Migração para Produção (`compliance.continuous_audit`)

Objetivo: mover o sistema V2 **inteiramente** para `compliance.continuous_audit` e
**desativar o sandbox**. Ao final, nada de operacional roda em `sandbox.grc`.

## Contexto

- `compliance.continuous_audit` **já tem** as `tb_incidents_*` históricas (dados de produção do V1)
  e o `tb_tests_executions` do V1. As `tb_incidents_*` têm `ArchiveDate` ✅.
- Como os `output_table` do seed **já batem** com os nomes dessas tabelas, **não há cópia de dados**:
  apontar o V2 para lá já traz todo o histórico (dashboards, análise de achados).
- Falta criar o **plano de controle do V2** (`tb_test_configurations`, hashes, suppressions,
  false_positives, dashboards) nesse schema.

## ⚠️ Regra de ouro

**Só UM orquestrador pode escrever nas `tb_incidents_*` / `tb_tests_executions`.**
O orquestrador V2 tem que **substituir** o V1 — nunca rodar em paralelo (senão duplica achados).
A virada de escrita é o **Passo 4**.

---

## Pré-requisitos

- [ ] Service principal do app com **read/write** em `compliance.continuous_audit`.
- [ ] Job de produção do orquestrador fará `%run` do **utils atualizado** (com `reduce` + FP por critérios).
- [ ] Decisão: o app aponta para **um** ambiente por vez. Ao virar, deixa de mostrar o sandbox.

---

## Passo 0 — Código (feito)

- `utils.py`: `CATALOG`/`SCHEMA` vêm de `CA_CATALOG`/`CA_SCHEMA` (default `sandbox`/`grc`).
- `run-all-tests.py`: `CONFIG_TABLE` deriva do `CATALOG`/`SCHEMA` do utils.
- `app`: já parametrizado por `CA_CATALOG`/`CA_SCHEMA` no `app.yaml`.

Durante a migração, prod é **sempre explícito** (widgets/env = `compliance`/`continuous_audit`);
os defaults só viram `compliance` no Passo 6.

## Passo 1 — Criar as tabelas de controle em produção

Rode `Setup/setup-tables.sql` com widgets:

- `catalog = compliance`
- `schema  = continuous_audit`

Cria só o que falta. **Não toca** nas `tb_incidents_*` nem no `tb_tests_executions`
(é `CREATE IF NOT EXISTS`).

## Passo 2 — Atualizar o schema do `tb_tests_executions` do V1

O `tb_tests_executions` de produção é do V1 e **não tem** as colunas novas
(`IncidentCountRaw`, `IsSupressed`, `IsRecurrent`, `IsContinued`). O app lê essas colunas —
sem elas, as queries de stats/dashboard **quebram**. Rode este snippet (idempotente):

```python
t = "compliance.continuous_audit.tb_tests_executions"
cols = [("IncidentCountRaw","INT"),("IsSupressed","BOOLEAN"),
        ("IsRecurrent","BOOLEAN"),("IsContinued","BOOLEAN")]
existing = {c.name for c in spark.table(t).schema}
for name, typ in cols:
    if name not in existing:
        spark.sql(f"ALTER TABLE {t} ADD COLUMNS ({name} {typ})")
        print("added", name)
print("OK — schema de tb_tests_executions atualizado")
```

Linhas históricas ficam com `NULL` nessas colunas (o app trata com COALESCE / status básico).

## Passo 3 — Semear as configs dos testes em produção

Rode `Setup/seed-v1-tests.py` com widgets:

- `catalog = compliance`
- `schema  = continuous_audit`
- `force_daily = false`  ← **frequências reais** (nunca diário em prod)

Popula `tb_test_configurations` com os 28 testes (25 ACTIVE, 3 PAUSED). Os `output_table`
já apontam para as `tb_incidents_*` históricas → o app lê o histórico na hora.

## Passo 4 — Virar a escrita (orquestrador)

*(Projeto separado do Job.)* Suba o Job V2 do orquestrador apontando para
`compliance.continuous_audit`:

- Env do cluster: `CA_CATALOG=compliance`, `CA_SCHEMA=continuous_audit`.
- `%run` do utils atualizado.
- Schedule 06:00 UTC diário.
- **Desative/remova o Job V1** no mesmo movimento (regra de ouro).

Primeira execução: apenda achados novos nas `tb_incidents_*` e loga em `tb_tests_executions`
(já com as colunas novas).

## Passo 5 — Virar o app para produção

No `app.yaml`, troque:

```yaml
- name: CA_CATALOG
  value: "compliance"
- name: CA_SCHEMA
  value: "continuous_audit"
```

Redeploy do app. **Só depois** dos passos 1–3 (as tabelas de controle precisam existir).
Valide na tela: 28 testes aparecem, "Analisar Achados" mostra o histórico, dashboards com dados reais.

## Passo 6 — Consolidar defaults e desativar o sandbox

Depois de tudo validado em produção:

- [ ] Trocar os defaults de `CA_CATALOG`/`CA_SCHEMA` para `compliance`/`continuous_audit`
      (utils.py, app.yaml) e os defaults dos widgets (`setup-tables.sql`, `seed-v1-tests.py`, `reset-data.sql`).
- [ ] Confirmar que nada mais escreve em `sandbox.grc`.
- [ ] (Opcional) `DROP`/arquivar as tabelas de `sandbox.grc` — usar `reset-data.sql` como referência.

---

## Rollback

- App: reverter `app.yaml` para `sandbox`/`grc` e redeploy.
- Orquestrador: reativar o Job V1 (se ainda não removido) — mas **nunca os dois ativos juntos**.
- As tabelas de controle criadas em produção são inertes se o app/orquestrador não apontarem para lá.

## Checklist de verificação pós-virada

- [ ] `SELECT count(*) FROM compliance.continuous_audit.tb_test_configurations` = 28
- [ ] App lista 25 ACTIVE + 3 PAUSED
- [ ] "Analisar Achados" de um teste mostra linhas históricas com datas antigas
- [ ] Snippet de ERROR do `tb_tests_executions` após 1ª execução V2 = 0 (ou só acesso a fonte)
- [ ] Nenhum Job V1 ativo
