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

### Example rule prompt

"table telemetry column temperature timestamp ts above 80 every 10s"

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
