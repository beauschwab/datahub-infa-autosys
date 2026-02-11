# AutoSys (JIL)

**Plugin:** `autosys_jil` · **Source class:** `AutoSysJilSource`

Parses AutoSys JIL (Job Information Language) text exports, emitting DataFlow/DataJob entities with job→job dependencies and optional bridging to Informatica workflows.

## How It Works

```
JIL text file → parse_jil()
  → DataFlow (box)
    → DataJob (job)
      → job→job dependency edges (from condition: strings)
      → optional bridge edge → Informatica DataJob
```

1. The JIL parser reads exported `.jil` text files
2. Boxes become DataFlows, jobs become DataJobs
3. `condition:` strings are parsed to build job→job dependency edges
4. If `bridge_to_informatica` is enabled, jobs whose commands reference Informatica workflows get cross-platform edges

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **jil_paths** | `list[str]` | *required* | Paths to `.jil` files to parse |
| platform | `str` | `"autosys"` | Platform for AutoSys URNs |
| env | `str` | `"PROD"` | Environment |
| bridge_to_informatica | `bool` | `true` | Create edges to Informatica DataJobs |
| informatica_job_name_regex | `str` | `(?i)\b(infa\|informatica)\b.*` | Regex to detect Informatica workflow names |
| informatica_dataflow_platform | `str` | `"informatica"` | Platform for Informatica bridge URNs |
| informatica_dataflow_env | `str` | `"PROD"` | Environment for Informatica bridge URNs |
| informatica_dataflow_id | `str` | `"default"` | DataFlow ID for Informatica bridge URNs |

### Example Recipe

```yaml
source:
  type: autosys_jil
  config:
    jil_paths:
      - /data/export/autosys.jil
    platform: autosys
    env: PROD
    bridge_to_informatica: true
    informatica_dataflow_platform: informatica
    informatica_dataflow_env: PROD
    informatica_dataflow_id: FINANCE_2052A

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

## Emitted Entities

| Entity | URN Pattern | Notes |
|--------|-------------|-------|
| DataFlow | `urn:li:dataFlow:(autosys,BOX_NAME,PROD)` | One per box |
| DataJob | `urn:li:dataJob:(urn:li:dataFlow:(...),JOB_NAME)` | One per job in the box |

## AutoSys ↔ Informatica Bridging

When `bridge_to_informatica: true`, the source examines each AutoSys job's `command:` field. If it matches the `informatica_job_name_regex`, a `custom_properties` field (`executes_informatica_job_urn`) is set, and an edge is created to the corresponding Informatica DataJob.

This lets you visualize the full orchestration chain: **AutoSys box → AutoSys job → Informatica workflow → Informatica mapping → datasets**.
