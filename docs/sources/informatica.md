# Informatica (pmrep)

**Plugin:** `infa_pmrep` · **Source class:** `InformaticaPmrepSource`

Exports and parses Informatica PowerCenter objects via the `pmrep` CLI, emitting DataFlow/DataJob/dataset entities with SQL-based lineage inference.

## How It Works

```
pmrep CLI → export folder XML → parse_informatica_folder_xml()
  → DataFlow (folder)
    → DataJob (workflow / session / mapping)
      → inputDatasets, outputDatasets
      → SQL snippets → inferred column lineage
```

1. `PmrepRunner` connects to the Informatica domain and exports the specified folder to XML
2. `parse_informatica_folder_xml()` parses the XML into structured mappings and workflows
3. The source emits DataFlows, DataJobs, and datasets with lineage edges
4. SQL snippets embedded in transformations are parsed for column-level lineage (optional)

## Configuration

### Full Config Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **pmrep.bin_path** | `str` | *required* | Absolute path to `pmrep` binary |
| **pmrep.domain** | `str` | *required* | Informatica domain name |
| **pmrep.repo** | `str` | *required* | Repository name |
| **pmrep.user** | `str` | *required* | Repository user |
| **pmrep.password_env** | `str` | *required* | Env var containing the password |
| pmrep.connect_timeout_s | `int` | `60` | pmrep connect timeout |
| pmrep.extra_env | `dict` | `{}` | Extra env vars for pmrep subprocess |
| **export.folder** | `str` | *required* | Folder/project to export |
| **export.out_dir** | `str` | *required* | Output directory for XML artifacts |
| export.include_workflows | `bool` | `true` | Export workflows |
| export.include_mappings | `bool` | `true` | Export mappings |
| export.overwrite | `bool` | `true` | Overwrite existing files |
| export.object_name_filter | `str?` | `null` | Regex filter for object names |
| lineage.lineage.platform | `str` | `"oracle"` | Platform for emitted datasets |
| lineage.lineage.platform_instance | `str?` | `null` | Platform instance |
| lineage.lineage.env | `str` | `"PROD"` | Environment |
| lineage.lineage.default_db | `str?` | `null` | Default DB for SQL parsing |
| lineage.lineage.default_schema | `str?` | `null` | Default schema for SQL parsing |
| lineage.infer_from_sql | `bool` | `true` | Infer lineage from embedded SQL |
| lineage.infer_from_mapping_ports | `bool` | `true` | Infer column lineage from mapping ports |
| lineage.attach_transformation_text | `bool` | `true` | Store raw SQL in custom properties |
| lineage.max_sql_snippet_chars | `int` | `20000` | Max SQL snippet length |
| dataflow_platform | `str` | `"informatica"` | Platform for DataFlow URNs |
| dataflow_env | `str` | `"PROD"` | Environment for DataFlow URNs |

### Example Recipe

```yaml
source:
  type: infa_pmrep
  config:
    pmrep:
      bin_path: /opt/informatica/server/bin/pmrep
      domain: YOUR_DOMAIN
      repo: YOUR_REPO
      user: YOUR_USER
      password_env: INFA_PASSWORD
    export:
      folder: FINANCE_2052A
      out_dir: /tmp/infa_export
      include_workflows: true
      include_mappings: true
    lineage:
      lineage:
        platform: oracle
        platform_instance: PROD
        default_db: DW
        default_schema: REPORTING
        ignore_table_patterns:
          - DUMMY_%
          - TEMP_%
      infer_from_sql: true
      infer_from_mapping_ports: true

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

## Emitted Entities

| Entity | URN Pattern | Notes |
|--------|-------------|-------|
| DataFlow | `urn:li:dataFlow:(informatica,FOLDER,PROD)` | One per exported folder |
| DataJob | `urn:li:dataJob:(urn:li:dataFlow:(...),workflow_name)` | Workflows, sessions, mappings |
| Dataset | `urn:li:dataset:(urn:li:dataPlatform:oracle,DB.SCHEMA.TABLE,PROD)` | Inputs/outputs per job |

## Notes

- The `pmrep` CLI must be available on the machine running the ingestion
- Password is read from an environment variable (not stored in the recipe)
- SQL snippets are truncated and hashed for `custom_properties` to avoid oversized events
- The `sql_snippets_preview` / `sql_snippets_sha256` pattern keeps stored-procedure text manageable
