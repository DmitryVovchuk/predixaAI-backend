# Rule Monitoring Microservices

This repository contains a control-plane rule service and a data-plane scheduler service for rule-based DB monitoring. The rule service parses rule prompts into a strict JSON spec and persists rules, while the scheduler validates rules with MCP and executes monitoring jobs.

## Services

- `rule-service` (HTTP control plane)
- `scheduler-service` (MCP-only data plane)
- `postgres-mcp` + `mysql-mcp` (per-DB MCP servers)

Scheduler admin endpoints:

- `GET /healthz`
- `GET /jobs`
- `POST /jobs/reload`

## API (rule-service)

- `POST /connections`
- `POST /rules/validate`
- `POST /rules`
- `GET /rules`
- `GET /rules/{id}`
- `PUT /rules/{id}`
- `POST /rules/{id}/enable`
- `POST /rules/{id}/disable`
- `GET /rules/{id}/alerts`
- `POST /alerts/{id}/treated`

## API (db-connector)

- `POST /connection/test`
- `POST /tables`
- `POST /describe`
- `POST /sample`
- `POST /profile`

### Strict connectionRef requests

List tables (connectionRef required):

```
curl -X POST http://localhost:8085/tables \
  -H 'Content-Type: application/json' \
  -d '{"connectionRef":"<uuid>","schema":"app"}'
```

Describe table (connectionRef required):

```
curl -X POST http://localhost:8085/describe \
  -H 'Content-Type: application/json' \
  -d '{"connectionRef":"<uuid>","table":"users"}'
```
- `POST /machine-units`
- `GET /machine-units`
- `GET /machine-units/{unitId}`
- `PUT /machine-units/{unitId}`
- `DELETE /machine-units/{unitId}`
- `POST /machine-units/{unitId}/rules`
- `POST /machine-units/{unitId}/columns`
- `PUT /machine-units/{unitId}/columns`
- `PUT /machine-units/{unitId}/table`
- `PUT /machine-units/{unitId}/connection`

### Example rule prompts

- "table telemetry column temperature timestamp ts above 80 every 10s"
- "table telemetry column temperature timestamp ts abnormal last 5m"
- "table telemetry column temperature timestamp ts missing"

## Example requests

Create connection:

```
curl -X POST http://localhost:8090/connections \
  -H 'Content-Type: application/json' \
  -d '{"name":"prod","type":"postgres","host":"db","port":5432,"user":"app","password":"secret","database":"app"}'
```

Validate rule:

```
curl -X POST http://localhost:8090/rules/validate \
  -H 'Content-Type: application/json' \
  -d '{"rulePrompt":"table telemetry column temperature timestamp ts above 80 every 10s","connectionRef":"<uuid>"}'

```

Validate rule with draft hints (recommended for ambiguity):

```
curl -X POST http://localhost:8090/rules/validate \
  -H 'Content-Type: application/json' \
  -d '{"rulePrompt":"abnormal","connectionRef":"<uuid>","draft":{"table":"telemetry","timestampColumn":"ts","parameters":[{"parameterName":"temperature","valueColumn":"temperature"}]}}'
```

Create rule with parameters array:

```
curl -X POST http://localhost:8090/rules \
  -H 'Content-Type: application/json' \
  -d '{"rulePrompt":"between 20 and 40","connectionRef":"<uuid>","draft":{"table":"telemetry","timestampColumn":"ts","parameters":[{"parameterName":"temperature","valueColumn":"temperature","detector":{"type":"threshold","threshold":{"op":"between","min":20,"max":40}}}]}}'
```

## Machine units

Create machine unit:

```
curl -X POST http://localhost:8090/machine-units \
  -H 'Content-Type: application/json' \
  -d '{"unitName":"cnc","connectionRef":"<uuid>","selectedTable":"etchers_data","selectedColumns":["gas_ar_flow","rf_power"],"rule":["<rule-uuid>"]}'
```

Add/remove columns:

```
curl -X POST http://localhost:8090/machine-units/<unitId>/columns \
  -H 'Content-Type: application/json' \
  -d '{"add":["gas_ch3f_flow"],"remove":["gas_ar_flow"]}'
```

Replace columns:

```
curl -X PUT http://localhost:8090/machine-units/<unitId>/columns \
  -H 'Content-Type: application/json' \
  -d '{"selectedColumns":["gas_ch3f_flow","rf_power"]}'
```

Change table (clears columns by default):

```
curl -X PUT http://localhost:8090/machine-units/<unitId>/table \
  -H 'Content-Type: application/json' \
  -d '{"selectedTable":"etchers_data"}'
```

Add/remove rules:

```
curl -X POST http://localhost:8090/machine-units/<unitId>/rules \
  -H 'Content-Type: application/json' \
  -d '{"add":["<rule-uuid>"],"remove":["<rule-uuid>"]}'
```
```

## Statuses

- `DRAFT` - rule persisted but not yet validated by scheduler
- `ACTIVE` - runtime validated and scheduled
- `INVALID` - runtime validation failed (see `last_error`)
- `DISABLED` - manually disabled

## MCP adapters

The scheduler talks to per-DB MCP servers using a lightweight adapter map. Configure them via `mcp.yaml` (recommended) or env overrides.

`mcp.yaml` (default compose mount):

- `adapters.postgres.endpoint`: `http://postgres-mcp:9001/rpc`
- `adapters.mysql.endpoint`: `http://mysql-mcp:9002/rpc`

Scheduler env options:

- `MCP_CONFIG_PATH` (optional path to `mcp.yaml`)
- `MCP_POSTGRES_HTTP` / `MCP_MYSQL_HTTP` (HTTP endpoints when no config file is used)
- `ALLOWLIST_TABLES` (comma-separated table allowlist)

MCP server env options:

- `MCP_DB_TYPE` (`postgres` or `mysql`)
- `DATABASE_URL` (rules database DSN for fetching connections)
- `ENCRYPTION_KEY` (AES-GCM key for decrypting stored passwords)
- `PORT` (HTTP listen port)

## Development

```
make migrate
make run-rule
make run-scheduler
make test
```
