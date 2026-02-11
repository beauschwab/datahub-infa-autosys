# Recipe Reference

Recipes are YAML files that configure a DataHub ingestion pipeline — specifying a **source** (what to ingest), an optional **transformer** pipeline, and a **sink** (where to emit). This is the standard [DataHub recipe format](https://datahubproject.io/docs/metadata-ingestion/#recipes).

## Structure

```yaml
source:
  type: <plugin_name>     # one of: infa_pmrep, autosys_jil, oracle_operational, essbase, ssis_dtsx, abinitio
  config:
    # source-specific configuration

# optional transformers
transformers:
  - type: simple_add_dataset_tags
    config:
      tag_urns:
        - "urn:li:tag:my_tag"

sink:
  type: datahub-rest       # or datahub-kafka, file, etc.
  config:
    server: "http://localhost:8080"
```

## Running a Recipe

=== "DataHub CLI"

    ```bash
    datahub ingest -c recipe.yml
    ```

=== "Project CLI"

    ```bash
    dhcs ingest -c recipe.yml
    dhcs ingest -c recipe.yml --dry-run
    ```

=== "Python"

    ```python
    from datahub_custom_sources import run_recipe
    run_recipe("recipe.yml")
    ```

=== "Airflow"

    ```python
    from datahub_custom_sources.airflow import DataHubIngestionOperator
    DataHubIngestionOperator(task_id="run", recipe_path="recipe.yml")
    ```

## Source Recipes

### Informatica (`infa_pmrep`)

```yaml
source:
  type: infa_pmrep
  config:
    pmrep:
      bin_path: "/opt/informatica/server/bin/pmrep"
      domain: "INFA_DOM"
      repository: "REPO_DEV"
      user: "Administrator"
      password_env: "INFA_PASSWORD"
    exports:
      - folder: "ETL_Finance"
        out_dir: "/tmp/infa_export"
    lineage:
      platform: "oracle"
      platform_instance: "PROD"
      env: "PROD"
      default_db: "DW"
      default_schema: "REPORTING"

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

### AutoSys (`autosys_jil`)

```yaml
source:
  type: autosys_jil
  config:
    jil_paths:
      - "/data/autosys/full_dump.jil"
    bridge_to_informatica: true

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

### Oracle Operational (`oracle_operational`)

```yaml
source:
  type: oracle_operational
  config:
    dsn: "oracle-prod.company.com:1521/DWPROD"
    user: "DATAHUB_SVC"
    password_env: "ORACLE_PASSWORD"
    poll_interval_s: 60
    lookback_minutes: 180
    emit_queries: false
    emit_instances: true
    query_sources:
      - "v$sql"
    lineage:
      platform: "oracle"
      platform_instance: "PROD"
      env: "PROD"
      default_db: "DW"
      default_schema: "REPORTING"

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

### Essbase (`essbase`)

```yaml
source:
  type: essbase
  config:
    base_url: "https://essbase.company.com"
    user: "ESSBASE_USER"
    password_env: "ESSBASE_PASSWORD"
    verify_ssl: true
    timeout_s: 30
    applications: ["FINANCE"]
    include_members: true
    include_formulas: true
    include_calc_scripts: true
    include_load_rules: true
    dataflow_platform: "essbase"
    dataflow_env: "PROD"
    dataset_platform: "essbase"
    dataset_env: "PROD"

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

### SSIS (`ssis_dtsx`)

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

### Ab Initio (`abinitio`)

```yaml
source:
  type: abinitio
  config:
    graph_paths:
      - "C:/abinitio/graphs/load_customer.mp"
    graph_dirs:
      - "C:/abinitio/graphs"
    dml_paths:
      - "C:/abinitio/dml/customer.dml"
    dml_dirs:
      - "C:/abinitio/dml"
    xfr_paths:
      - "C:/abinitio/xfr/customer_map.xfr"
    xfr_dirs:
      - "C:/abinitio/xfr"
    io_mapping_paths:
      - "C:/abinitio/io_mapping.json"
    project_name: "CustomerProject"
    dataflow_platform: "abinitio"
    dataflow_env: "PROD"
    lineage:
      platform: "hdfs"
      env: "PROD"

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

## Common Lineage Config

Several sources share a `lineage` block with these fields:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| platform | `str` | `"oracle"` | DataHub platform for emitted datasets |
| platform_instance | `str?` | `null` | Platform instance qualifier |
| env | `str` | `"PROD"` | DataHub environment |
| default_db | `str?` | `null` | Default database name for SQL parsing |
| default_schema | `str?` | `null` | Default schema name for SQL parsing |

## Sink Types

Any standard DataHub sink works with these sources:

| Sink Type | When to Use |
|-----------|-------------|
| `datahub-rest` | Direct push to DataHub GMS REST API |
| `datahub-kafka` | Push via Kafka (for high-volume deployments) |
| `file` | Write MCPs to a JSON file (for debugging) |

## Tips

- Use `--dry-run` with the CLI to validate recipes without emitting
- Keep passwords in environment variables (use `password_env` fields)
- Start with the `file` sink to inspect output before pushing to DataHub
- Use `datahub check plugins` to verify your sources are installed correctly
