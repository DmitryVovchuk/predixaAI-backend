# Changelog (Dev)

## 2026-02-17
- **db-connector**: `/tables` and `/describe` now require `connectionRef` only (inline connection rejected). `/connection/test` still accepts inline credentials.
- **Resolver errors**: missing ref -> 400, not found -> 404, not configured -> 400.
- **Env vars** (db-connector when using connectionRef): `DATABASE_URL`, `ENCRYPTION_KEY` (32 bytes).
- **How to test**: `go test ./...`
- **Migrations**: none

### Example payloads

```json
{
  "connectionRef": "<uuid>"
}
```

```json
{
  "connection": {
    "type": "postgres",
    "host": "localhost",
    "port": 5432,
    "user": "app",
    "password": "secret",
    "database": "app"
  }
}
```

```json
{
  "connectionRef": "uuid-here",
  "schema": "app"
}
```

```json
{
  "connectionRef": "uuid-here",
  "table": "users"
}
```
