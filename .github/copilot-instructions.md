# Copilot instructions for datahub-custom-sources

## Big picture architecture
- This repo ships **six DataHub ingestion Source plugins**, a **programmatic runner API**, and an **Airflow operator** for scheduled metadata ingestion.
- Source plugins (all registered as `datahub ingest` entry-points in [pyproject.toml](pyproject.toml)):
  | Plugin name          | Source class                | File |
  |----------------------|-----------------------------|------|
  | `infa_pmrep`         | `InformaticaPmrepSource`    | [sources/informatica.py](src/datahub_custom_sources/sources/informatica.py) |
  | `autosys_jil`        | `AutoSysJilSource`          | [sources/autosys.py](src/datahub_custom_sources/sources/autosys.py) |
  | `oracle_operational` | `OracleOperationalSource`   | [sources/oracle_operational.py](src/datahub_custom_sources/sources/oracle_operational.py) |
  | `essbase`            | `EssbaseSource`             | [sources/essbase.py](src/datahub_custom_sources/sources/essbase.py) |
  | `ssis_dtsx`          | `SsisDtsxSource`            | [sources/ssis.py](src/datahub_custom_sources/sources/ssis.py) |
  | `abinitio`           | `AbInitioSource`            | [sources/abinitio.py](src/datahub_custom_sources/sources/abinitio.py) |

- **Informatica flow**: `PmrepRunner` exports folder XML â†’ `parse_informatica_folder_xml()` builds mappings/workflows â†’ emit DataFlow/DataJobs/datasets + lineage. See [extractors/pmrep.py](src/datahub_custom_sources/extractors/pmrep.py) and [extractors/informatica_xml.py](src/datahub_custom_sources/extractors/informatica_xml.py).
- **AutoSys flow**: JIL text parser builds jobs/boxes â†’ emit DataFlows and jobâ†’job dependencies from `condition:` strings. See [extractors/autosys_jil.py](src/datahub_custom_sources/extractors/autosys_jil.py).
- **Essbase flow**: REST API client fetches cubes, calc scripts, and load rules â†’ emit DataFlow/DataJob/dataset. See [extractors/essbase_client.py](src/datahub_custom_sources/extractors/essbase_client.py) and [extractors/essbase_schema.py](src/datahub_custom_sources/extractors/essbase_schema.py).
- **SSIS flow**: DTSX XML parser â†’ extract tasks + SQL statements â†’ emit DataFlow/DataJob/dataset lineage + optional column-level lineage via `schema_paths`. See [extractors/ssis_dtsx.py](src/datahub_custom_sources/extractors/ssis_dtsx.py).
- **Ab Initio flow**: DML schema parsing, graph file iteration, XFR mapping â†’ emit datasets/schema + DataJob per graph + optional IO mapping from JSON. See [extractors/abinitio.py](src/datahub_custom_sources/extractors/abinitio.py).
- **Operational lineage** is event-like and streamed via the reference runner using `DataProcessInstance`; the ingestion Source is intentionally minimal. See [operational/oracle_runner.py](src/datahub_custom_sources/operational/oracle_runner.py) and [docs/OPERATIONAL_LINEAGE.md](docs/OPERATIONAL_LINEAGE.md).

## Project structure

```
src/datahub_custom_sources/
â”œâ”€â”€ __init__.py          # Re-exports: run_recipe, run_recipe_dict, load_recipe
â”œâ”€â”€ runner.py            # Programmatic API (YAML or dict â†’ Pipeline) â€” main entry for Airflow
â”œâ”€â”€ cli.py               # Typer CLI: `dhcs ingest -c recipe.yml` + inspect commands
â”œâ”€â”€ config.py            # Pydantic v2 config models for all six sources
â”œâ”€â”€ airflow/
â”‚   â”œâ”€â”€ __init__.py      # Re-exports DataHubIngestionOperator
â”‚   â””â”€â”€ operators.py     # Airflow BaseOperator (Jinja, XCom, dry-run support)
â”œâ”€â”€ emit/
â”‚   â””â”€â”€ builders.py      # Shared MCP builder helpers (make_edge, mcp_dataflow_info, etc.)
â”œâ”€â”€ extractors/          # Per-system parsers (pmrep, JIL, DTSX, DML/XFR, Essbase REST)
â”œâ”€â”€ sources/             # DataHub Source plugins (one per system)
â”‚   â””â”€â”€ common.py        # SimpleReport, as_workunits() shared utilities
â”œâ”€â”€ operational/
â”‚   â””â”€â”€ oracle_runner.py # Oracle DataProcessInstance runner
â””â”€â”€ utils/
    â”œâ”€â”€ urns.py          # URN construction helpers
    â”œâ”€â”€ paths.py         # expand_paths() â€” shared dedup directory scanner
    â””â”€â”€ subprocess.py    # Shell command runner (stdlib logging)
```

## Three ways to run ingestion

1. **DataHub CLI** (standard): `datahub ingest -c examples/recipes/infa_pmrep.yml`
2. **Project CLI** (debug-friendly): `dhcs ingest -c examples/recipes/infa_pmrep.yml --dry-run`
3. **Python / Airflow** (programmatic):
   ```python
   from datahub_custom_sources import run_recipe          # from YAML file
   from datahub_custom_sources import run_recipe_dict      # from a dict
   from datahub_custom_sources.airflow import DataHubIngestionOperator  # Airflow operator
   ```
   See [runner.py](src/datahub_custom_sources/runner.py) and [airflow/operators.py](src/datahub_custom_sources/airflow/operators.py).

## Emission patterns & conventions
- Build DataHub MCP aspects using helpers in [emit/builders.py](src/datahub_custom_sources/emit/builders.py). Prefer dataset-level `UpstreamLineage` via `mcp_upstream_lineage()` (UI support is strongest there).
- Use URN helpers in [utils/urns.py](src/datahub_custom_sources/utils/urns.py) to keep URN construction consistent.
- Configuration lives in Pydantic v2 `ConfigModel`s; add new config fields to [config.py](src/datahub_custom_sources/config.py) and validate with `model_validate()` in Sources.
- For directory-scanning options (`*_dirs` config fields), use `expand_paths()` from [utils/paths.py](src/datahub_custom_sources/utils/paths.py) â€” do **not** hand-roll dedup loops in source files.
- AutoSys â†” Informatica bridging is done via `custom_properties` (`executes_informatica_job_urn`) in [sources/autosys.py](src/datahub_custom_sources/sources/autosys.py); keep this contract if adding new bridge behavior.
- SQL snippets are truncated and hashed for `custom_properties` in `InformaticaPmrepSource` to avoid oversized events; keep the `sql_snippets_preview`/`sql_snippets_sha256` pattern intact. See [sources/informatica.py](src/datahub_custom_sources/sources/informatica.py).
- All logging uses `logging.getLogger(__name__)` â€” Rich Console is **not** used in library code (Airflow compat).

## Developer workflows
- Install: `pip install -e ".[dev]"` (uses DataHub SDK + Pydantic v2). See [pyproject.toml](pyproject.toml).
- Install with Airflow support: `pip install -e ".[dev,airflow]"`.
- Tests: `pytest -q`.
- Run via DataHub CLI using recipes in [examples/recipes](examples/recipes):
  - `datahub ingest -c examples/recipes/infa_pmrep.yml`
  - `datahub ingest -c examples/recipes/autosys_jil.yml`
  - `datahub ingest -c examples/recipes/oracle_operational.yml`
  - `datahub ingest -c examples/recipes/essbase.yml`
  - `datahub ingest -c examples/recipes/ssis_dtsx.yml`
  - `datahub ingest -c examples/recipes/abinitio.yml`
- Local debug CLI: `dhcs ingest -c recipe.yml [--dry-run]` or `dhcs inspect-informatica â€¦` / `dhcs inspect-autosys â€¦`. See [cli.py](src/datahub_custom_sources/cli.py).
- Airflow example DAG: [examples/airflow/metadata_ingestion_dag.py](examples/airflow/metadata_ingestion_dag.py).

## External integration points
- `pmrep` CLI requires an env var password (`PmrepConfig.password_env`) and may differ by Informatica version; keep `PmrepRunner` conservative. See [extractors/pmrep.py](src/datahub_custom_sources/extractors/pmrep.py).
- Oracle operational lineage uses `oracledb` and expects `OracleOperationalConfig.password_env` for credentials. See [operational/oracle_runner.py](src/datahub_custom_sources/operational/oracle_runner.py).
- Essbase uses `requests` against the REST API; connection config is in `EssbaseSourceConfig`. See [extractors/essbase_client.py](src/datahub_custom_sources/extractors/essbase_client.py).
- SSIS and Ab Initio rely on file-based parsing (no live connections). Point `*_paths` or `*_dirs` config fields at exported artifacts.

## Adding a new source plugin
1. Create a config class in [config.py](src/datahub_custom_sources/config.py) inheriting from `pydantic.BaseModel`.
2. Create an extractor in [extractors/](src/datahub_custom_sources/extractors/) for parsing the source system's artifacts.
3. Create a Source class in [sources/](src/datahub_custom_sources/sources/) implementing `datahub.ingestion.source.source.Source` with `create()`, `get_workunits()`, `get_report()`, `close()`.
4. Use `expand_paths()` from [utils/paths.py](src/datahub_custom_sources/utils/paths.py) for any directory-scanning config.
5. Use MCP helpers from [emit/builders.py](src/datahub_custom_sources/emit/builders.py) and URN helpers from [utils/urns.py](src/datahub_custom_sources/utils/urns.py).
6. Register the entry-point in `[project.entry-points."datahub.ingestion.source.plugins"]` in [pyproject.toml](pyproject.toml).
7. Add a recipe in [examples/recipes/](examples/recipes/).
8. Export the Source class in [sources/__init__.py](src/datahub_custom_sources/sources/__init__.py).
