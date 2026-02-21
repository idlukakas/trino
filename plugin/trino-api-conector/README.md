# trino-api-conector

Conector Trino para a API de Extracao Univesp CRM v2 (`https://crm.api.une.cx`).

## O que o conector expoe

Schema padrao: `univesp` (configuravel por `api.schema-name`).

Tabelas:

- `lista_lead` -> `/api/univesp/lista-lead`
- `lista_protocolo` -> `/api/univesp/lista-protocolo`
- `lista_protocolo_status` -> `/api/univesp/lista-protocolo-status`
- `lista_atendimento` -> `/api/univesp/lista-atendimento`
- `lista_contato_chat` -> `/api/univesp/lista-contato-chat`
- `lista_atendimento_chat` -> `/api/univesp/lista-atendimento-chat`
- `lista_mensagem_atendimento_chat` -> `/api/univesp/lista-mensagem-atendimento-chat`
- `lista_atendimento_chatbot` -> `/api/univesp/lista-atendimento-chatbot`
- `lista_mensagem_atendimento_chatbot` -> `/api/univesp/lista-mensagem-atendimento-chatbot`

Cada tabela inclui:

- Colunas principais documentadas no PDF.
- Coluna `raw_json` com o registro completo retornado pela API (para campos extras nao documentados).

## Como funciona o filtro de data

- A API exige `data_inicio` e `data_termino`.
- O conector tenta usar o filtro SQL na coluna temporal principal da tabela (`dt_criacao`, `dt_mensagem`, etc.).
- A consulta e quebrada automaticamente em blocos de ate 31 dias (`api.max-days-per-request`).
- Sem filtro na query, o conector usa:
  - `api.default-start-date`/`api.default-end-date`, se definidos; ou
  - janela relativa de `api.default-lookback-days`.

## Build do plugin

No root do repositorio:

```bash
./mvnw -f plugin/trino-api-conector/pom.xml clean package
```

Artefato esperado:

- `plugin/trino-api-conector/target/trino-api-conector-480-SNAPSHOT.zip`

## Deploy em uma instancia Trino

1. Copie o zip para o host do Trino.
2. Extraia em `plugin/` do Trino (diretorio do servidor):
   - vai criar `plugin/trino-api-conector/` com jars.
3. Copie `plugin/trino-api-conector/catalog/univesp.properties` para `etc/catalog/univesp.properties`.
4. Ajuste `api.auth-code` se necessario.
5. Reinicie o Trino.

## Exemplo de query

```sql
SELECT id_lead, dt_criacao, r1, r2
FROM univesp.lista_lead
WHERE dt_criacao >= TIMESTAMP '2025-12-01 00:00:00'
  AND dt_criacao < TIMESTAMP '2025-12-16 00:00:00';
```

```sql
SELECT id_mensagens_atendimento, dt_mensagem, ds_mensagem, raw_json
FROM univesp.lista_mensagem_atendimento_chat
WHERE dt_mensagem >= TIMESTAMP '2025-12-01 00:00:00'
  AND dt_mensagem < TIMESTAMP '2025-12-05 00:00:00';
```
