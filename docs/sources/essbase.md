# Essbase

**Plugin:** `essbase` · **Source class:** `EssbaseSource`

Connects to Oracle Essbase via REST API to extract cubes, dimensions, calc scripts, and load rules — emitting DataFlow/DataJob/dataset entities with schema metadata.

## How It Works

```
Essbase REST API → fetch applications, cubes, calc scripts, load rules
  → DataFlow (application)
    → DataJob (cube / calc script / load rule)
      → Dataset (cube) with dimension schema
```

1. Connects to Essbase REST API using the configured credentials
2. Fetches applications (optionally filtered) and their cubes
3. Extracts dimension members, formulas, calc scripts, and load rules
4. Emits DataFlows per application, DataJobs per cube/script/rule, and datasets with schema

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **base_url** | `str` | *required* | Essbase REST API base URL |
| **user** | `str` | *required* | Essbase username |
| **password_env** | `str` | *required* | Env var containing Essbase password |
| verify_ssl | `bool` | `true` | Verify SSL certificates |
| timeout_s | `int` | `30` | HTTP timeout in seconds |
| applications | `list[str]?` | `null` (all) | Filter to specific applications |
| cubes | `list[str]?` | `null` (all) | Filter to specific cubes |
| include_members | `bool` | `true` | Include dimension members in schema |
| include_formulas | `bool` | `true` | Fetch member formulas |
| include_calc_scripts | `bool` | `true` | Emit calc scripts as DataJobs |
| include_load_rules | `bool` | `true` | Emit load rules as DataJobs |
| calc_script_max_chars | `int` | `20000` | Max chars of calc script in properties |
| load_rule_max_chars | `int` | `20000` | Max chars of load rule SQL in properties |
| dataflow_platform | `str` | `"essbase"` | Platform for DataFlow URNs |
| dataflow_env | `str` | `"PROD"` | Environment for DataFlow URNs |
| dataset_platform | `str` | `"essbase"` | Platform for dataset URNs |
| dataset_platform_instance | `str?` | `null` | Platform instance for datasets |
| dataset_env | `str` | `"PROD"` | Environment for datasets |

### Example Recipe

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

## Emitted Entities

| Entity | URN Pattern | Notes |
|--------|-------------|-------|
| DataFlow | `urn:li:dataFlow:(essbase,APP_NAME,PROD)` | One per application |
| DataJob | `urn:li:dataJob:(...,CUBE_NAME)` | Cubes, calc scripts, load rules |
| Dataset | `urn:li:dataset:(urn:li:dataPlatform:essbase,APP.CUBE,PROD)` | With dimension schema |

## Notes

- The Essbase REST API must be accessible from the machine running ingestion
- Password is read from an environment variable for security
- Calc script content and load rule SQL are truncated to `*_max_chars` to avoid oversized events
