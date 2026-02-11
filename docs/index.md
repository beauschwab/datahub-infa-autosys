# datahub-custom-sources

**Custom DataHub ingestion source plugins** for six enterprise metadata systems — with a programmatic Python API and an Airflow operator for scheduled ingestion.

---

## Supported Sources

| Source plugin        | System              | What it emits |
|----------------------|---------------------|---------------|
| `infa_pmrep`         | Informatica (pmrep) | DataFlow → DataJob (workflow/session/mapping) → datasets + SQL lineage |
| `autosys_jil`        | AutoSys (JIL)       | DataFlow (box) → DataJob (job) → dependencies + Informatica bridging |
| `oracle_operational` | Oracle DB           | `DataProcessInstance` runs with input/output datasets, run events |
| `essbase`            | Oracle Essbase      | DataFlow (app) → DataJob (cube/calc scripts/load rules) → dataset schema |
| `ssis_dtsx`          | SQL Server SSIS     | DataFlow (package) → DataJob (task) → SQL dataset + column lineage |
| `abinitio`           | Ab Initio           | DataFlow (project) → DataJob (graph) → DML schema + XFR/IO lineage |

## Three Ways to Run

=== "DataHub CLI"

    ```bash
    datahub ingest -c examples/recipes/infa_pmrep.yml
    ```

=== "Project CLI"

    ```bash
    dhcs ingest -c examples/recipes/infa_pmrep.yml --dry-run
    ```

=== "Python / Airflow"

    ```python
    from datahub_custom_sources import run_recipe
    run_recipe("examples/recipes/infa_pmrep.yml")
    ```

## Quick Install

```bash
pip install datahub-custom-sources              # core
pip install "datahub-custom-sources[airflow]"    # + Airflow operator
```

## Architecture Overview

```
src/datahub_custom_sources/
├── __init__.py          # Re-exports: run_recipe, run_recipe_dict, load_recipe
├── runner.py            # Programmatic API (YAML or dict → Pipeline)
├── cli.py               # Typer CLI: dhcs ingest + inspect commands
├── config.py            # Pydantic v2 config models for all six sources
├── airflow/
│   ├── __init__.py      # Re-exports DataHubIngestionOperator
│   └── operators.py     # Airflow 3.x BaseOperator (Jinja, XCom, dry-run)
├── emit/
│   └── builders.py      # Shared MCP builder helpers
├── extractors/          # Per-system parsers (pmrep, JIL, DTSX, DML/XFR, Essbase)
├── sources/             # DataHub Source plugins (one per system)
│   └── common.py        # SimpleReport, as_workunits()
├── operational/
│   └── oracle_runner.py # Oracle DataProcessInstance runner
└── utils/
    ├── urns.py          # URN construction helpers
    ├── paths.py         # expand_paths() — shared dedup directory scanner
    └── subprocess.py    # Shell command runner (stdlib logging)
```
