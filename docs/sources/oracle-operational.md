# Oracle Operational Lineage

**Plugin:** `oracle_operational` · **Source class:** `OracleOperationalSource`

Records what *actually ran* in Oracle — which datasets were read/written, timings, outcomes — emitting `DataProcessInstance` entities for runtime lineage.

## How It Works

```
Oracle DB (V$SQL / audit trail) → oracledb connection
  → DataProcessInstance (one per run/execution)
    → input datasets, output datasets
    → run events (STARTED, COMPLETE + SUCCESS/FAILURE)
```

1. Connects to Oracle via `oracledb` using the configured DSN and credentials
2. Queries `V$SQL`, `DBA_HIST_SQLTEXT`, or unified audit trail for recent activity
3. Parses SQL statements to identify input/output datasets
4. Emits `DataProcessInstance` entities with run metadata

!!! info "Operational vs. Structural Lineage"
    Unlike structural sources (Informatica, SSIS) that describe *what could happen*, operational lineage describes *what actually happened*. This source is best used alongside structural sources for a complete picture.

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **dsn** | `str` | *required* | Oracle DSN (e.g. `host/service_name`) |
| **user** | `str` | *required* | Oracle username |
| **password_env** | `str` | *required* | Env var containing Oracle password |
| poll_interval_s | `int` | `60` | Polling interval for new records |
| lookback_minutes | `int` | `180` | How far back to look on startup |
| emit_queries | `bool` | `false` | Also emit Query entities with SQL lineage |
| emit_instances | `bool` | `true` | Emit DataProcessInstance entities |
| query_sources | `list[str]` | `["v$sql"]` | Oracle sources to read queries from |
| lineage.platform | `str` | `"oracle"` | Platform for emitted datasets |
| lineage.platform_instance | `str?` | `null` | Platform instance |
| lineage.env | `str` | `"PROD"` | Environment |
| lineage.default_db | `str?` | `null` | Default database |
| lineage.default_schema | `str?` | `null` | Default schema |

### Example Recipe

```yaml
source:
  type: oracle_operational
  config:
    dsn: YOUR_HOST/YOUR_SERVICE
    user: YOUR_USER
    password_env: ORACLE_PASSWORD
    lineage:
      platform: oracle
      platform_instance: PROD
      env: PROD
      default_db: DW
      default_schema: REPORTING

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

## Oracle Data Sources

The source can read from multiple Oracle telemetry views:

| Source | What it provides |
|--------|-----------------|
| `v$sql` | Active SQL in the shared pool — latest executions |
| `dba_hist_sqltext` | AWR historical SQL text — broader time range |
| `unified_audit_trail` | Object-level access audit — who accessed what |

## Further Reading

See [OPERATIONAL_LINEAGE.md](../OPERATIONAL_LINEAGE.md) for detailed design notes on Oracle telemetry fields, mapping to DataHub entities, and implementation strategy.
