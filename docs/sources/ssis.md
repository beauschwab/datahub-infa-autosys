# SSIS (DTSX)

**Plugin:** `ssis_dtsx` · **Source class:** `SsisDtsxSource`

Parses SQL Server Integration Services `.dtsx` package files, extracting tasks and embedded SQL to emit DataFlow/DataJob/dataset entities with optional column-level lineage.

## How It Works

```
.dtsx XML files → parse SSIS package XML
  → DataFlow (package)
    → DataJob (task: Execute SQL, Data Flow, etc.)
      → dataset inputs/outputs from SQL parsing
      → optional column-level lineage via schema_paths
```

1. Reads `.dtsx` XML files from explicit paths and/or directories (`dtsx_dirs`)
2. Parses the SSIS package XML namespace to extract tasks and their SQL statements
3. Uses `sqlglot` to parse SQL and identify source/target tables
4. If `schema_paths` are provided, enriches lineage with column-level mappings
5. Emits DataFlows per package, DataJobs per task, datasets with lineage

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **dtsx_paths** | `list[str]` | *required* | Paths to `.dtsx` package files |
| dtsx_dirs | `list[str]` | `[]` | Directories to scan recursively for `.dtsx` files |
| schema_paths | `list[str]` | `[]` | JSON files mapping table names to column lists |
| dataflow_platform | `str` | `"ssis"` | Platform for DataFlow URNs |
| dataflow_env | `str` | `"PROD"` | Environment for DataFlow URNs |
| parse_sql | `bool` | `true` | Extract dataset lineage from embedded SQL |
| sql_max_chars | `int` | `20000` | Max SQL text in custom properties |
| lineage.platform | `str` | `"oracle"` | Platform for emitted datasets |
| lineage.env | `str` | `"PROD"` | Environment for datasets |
| lineage.default_db | `str?` | `null` | Default database for SQL parsing |
| lineage.default_schema | `str?` | `null` | Default schema for SQL parsing |

### Example Recipe

```yaml
source:
  type: ssis_dtsx
  config:
    dtsx_paths:
      - "C:/ssis/packages/finance_etl.dtsx"
    dtsx_dirs:
      - "C:/ssis/packages"
    schema_paths:
      - "C:/ssis/schema_mapping.json"
    dataflow_platform: "ssis"
    dataflow_env: "PROD"
    parse_sql: true
    lineage:
      platform: "mssql"
      env: "PROD"
      default_db: "DW"
      default_schema: "dbo"

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

## Directory Scanning

Use `dtsx_dirs` to automatically discover all `.dtsx` files in a directory tree:

```yaml
config:
  dtsx_paths: []          # can be empty if using dirs
  dtsx_dirs:
    - "/opt/ssis/packages"
    - "/opt/ssis/shared"
```

Files found via `dtsx_dirs` are de-duplicated with any explicit `dtsx_paths` using the shared `expand_paths()` utility.

## Column-Level Lineage

To enable column-level lineage, provide `schema_paths` — JSON files that map table names to their column lists:

```json
{
  "DW.dbo.customers": ["customer_id", "name", "email", "created_at"],
  "DW.dbo.orders": ["order_id", "customer_id", "amount", "order_date"]
}
```

When schema context is available, the SQL parser can trace column-level data flow from source to target tables.

## Emitted Entities

| Entity | URN Pattern | Notes |
|--------|-------------|-------|
| DataFlow | `urn:li:dataFlow:(ssis,PACKAGE_NAME,PROD)` | One per `.dtsx` package |
| DataJob | `urn:li:dataJob:(...,TASK_NAME)` | Execute SQL Tasks, Data Flow Tasks |
| Dataset | `urn:li:dataset:(urn:li:dataPlatform:mssql,DB.SCHEMA.TABLE,PROD)` | Tables from SQL parsing |

## Further Reading

See [SSIS.md](../SSIS.md) for detailed design notes on DTSX XML parsing strategies and open-source tooling.
