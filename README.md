# DB Connector Service

A lightweight HTTP service that exposes multi-database metadata and profiling operations for MySQL, PostgreSQL, and MSSQL.

## Endpoints

- `GET /health`
- `POST /tables` — `{ "connection": { ... } }`
- `POST /describe` — `{ "connection": { ... }, "table": "schema.table" }`
- `POST /sample` — `{ "connection": { ... }, "table": "schema.table", "limit": 50 }`
- `POST /profile` — `{ "connection": { ... }, "table": "schema.table", "options": { "maxColumns": 25, "sampleLimit": 50 } }`

### Connection object

```
{
  "type": "mysql|postgres|mssql",
  "host": "localhost",
  "port": 5432,
  "user": "dbuser",
  "password": "secret",
  "database": "mydb",
  "sslMode": "disable"
}
```

## Run locally

```
go test ./...
PORT=8080 go run ./cmd/service
```

## Docker

```
docker build -t db-connector .
docker run -p 8080:8080 db-connector
```
