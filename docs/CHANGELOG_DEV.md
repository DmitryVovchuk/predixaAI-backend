# Changelog (Dev)

## 2026-02-18
- **rule-service**: machine-units CRUD now supports `timestampColumn` (persisted on machine_units).
- **rule-service**: stepper parameters now prefer machine-unit `timestampColumn` when valid.
- **rule-service + scheduler-service**: Phase 1 detector types supported: `spec_limit`, `shewhart`, `range_chart`, `trend`, `tpa`.
- **rule-service**: added Rule Creation Stepper endpoints (`/api/rules/catalog`, `/api/rules/preview`, `/api/rules/baseline/check`, CRUD, parameters, health).
- **scheduler-service**: preview/baseline check endpoints on admin server for stepper.
- **scheduler-service**: standardized alert metadata includes window/baseline ranges, violations array, and computed stats.
- **scheduler-service**: trend continuity uses timestamp gaps (strictly increasing; gaps >2x median invalid). TPA defaults to timestamp regression basis.
- **range_chart**: subgroupSize limited to 2–10 (D3/D4 constants) and validated in rule-service + scheduler runtime validation.
- **Validation**: new config validation for baseline windows, subgrouping, sigma multipliers, and TPA thresholds.
- **How to test**: `go test ./...`
- **Quality gate**: `go vet ./...`
- **Migrations**: `009_add_machine_unit_timestamp_column.sql`

### Example payloads

```json
{
  "detector": {
    "type": "spec_limit",
    "specLimit": {
      "mode": "spec",
      "specLimits": {"usl": 100, "lsl": 10}
    }
  }
}
```

```json
{
  "detector": {
    "type": "shewhart",
    "shewhart": {
      "baseline": {"lastN": 50},
      "sigmaMultiplier": 3,
      "minBaselineN": 20
    }
  }
}
```

```json
{
  "detector": {
    "type": "range_chart",
    "rangeChart": {
      "subgroupSize": 2,
      "subgrouping": {"mode": "consecutive"},
      "baseline": {"lastN": 50},
      "minBaselineSubgroups": 5
    }
  }
}
```

```json
{
  "detector": {
    "type": "trend",
    "trend": {
      "windowSize": 6,
      "epsilon": 0
    }
  }
}
```

```json
{
  "detector": {
    "type": "tpa",
    "tpa": {
      "windowN": 5,
      "regressionTimeBasis": "index",
      "slopeThreshold": 0.5
    }
  }
}
```

## 2026-02-17
- **db-connector**: `/tables` and `/describe` now require `connectionRef` only (inline connection rejected). `/connection/test` still accepts inline credentials.
- **Resolver errors**: missing ref -> 400, not found -> 404, not configured -> 400.
- **Env vars** (db-connector when using connectionRef): `DATABASE_URL`, `ENCRYPTION_KEY` (32 bytes).
- **How to test**: `go test ./...`
- **Migrations**: none

- **rule-service**: machine-units `rule` payload accepts an empty object `{}` and treats it as an empty list (backward-compatible).
- **rule-service**: machine-units POST now updates when `unitId` is provided and exists; otherwise `unitId` remains server-generated.

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
