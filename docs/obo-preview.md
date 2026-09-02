# Preview de query com a permissão do usuário (OBO)

O botão **Testar / Validar Query** (`POST /api/run-preview`, ramo `SQL`) executa
SQL escrito pelo próprio usuário. Desde 2026-09-01 ele roda com a **identidade de
quem clicou**, e não com a service principal do app.

**Por quê:** o preview roda `SELECT * FROM (<query do usuário>) LIMIT 10`. Se
isso rodasse na SP, o app seria um bypass do Unity Catalog — bastava colar uma
query no formulário para ler qualquer tabela que a SP enxerga.

Todo o resto do app (tabelas de controle do Continuous Audit) continua na SP.
Nada mudou fora do preview.

## Como funciona

| Peça | Onde |
|---|---|
| Token do usuário | header `X-Forwarded-Access-Token`, encaminhado pelo Databricks Apps |
| Conexão dedicada | `db.query_as()` — fora do pool por requisição (`_request_db`) |
| Sem token | falha com `error_kind: "no_token"`. **Não** há fallback para a SP |
| Dev local | sem `DATABRICKS_APP_PORT`, usa `DATABRICKS_TOKEN` (o PAT pessoal do dev) |

## Configuração obrigatória (fora do código)

O header só é encaminhado se o app declarar `user_api_scopes`. **Não é chave do
`app.yaml`** (que aceita só `command` e `env`) — vai no recurso do app:

```bash
databricks apps update continuous-audit --json '{"user_api_scopes": ["sql:restricted-query"]}'
```

`sql:restricted-query` é o escopo read-only, que combina com o allow-list de
`validation.py` (só `SELECT` / `WITH`).

Pré-requisitos:

1. Workspace admin liberar o escopo em `allowedAppsUserApiScopes`.
2. Reiniciar o app depois de habilitar user authorization no workspace pela primeira vez.
3. Cada usuário: `CAN USE` no warehouse `ed5c06d0f073810c` + consent uma vez
   (**o consent não pode ser revogado pelo usuário depois de dado**).

⚠️ **Ordem do rollout:** config → consent → deploy do código. Enquanto os escopos
não estiverem declarados, o header não chega e o preview falha para todo mundo
com "Autorização necessária".

## Consequências aceitas

- Na tela de aprovação, o `RunPreviewInline` roda com a permissão do **revisor**,
  não a do autor. Revisor sem acesso à tabela vê "Sem permissão" ao testar.
- O preview verde **não** garante que o job vai rodar: a execução real acontece
  no orquestrador (`run-all-tests.py`), com a identidade do job. Decidiu-se
  conscientemente **não** adicionar um gate que valide a identidade de execução.

## Erros

`_classify_query_error()` (main.py) traduz o erro cru em `error_kind`:
`no_token`, `permission_denied`, `not_found`, `bad_column`, `syntax`, `expired`,
`unknown`. A tela (`PreviewError`) mostra título + mensagem acionável e guarda o
texto original do Databricks num `<details>`.
