# Development Guide

## Prerequisites

- Python 3.10+
- Git

## Setup

```bash
git clone https://github.com/beauschwab/datahub-infa-autosys.git
cd datahub-infa-autosys

# Install with dev dependencies
pip install -e ".[dev]"

# Install with Airflow support for testing the operator
pip install -e ".[dev,airflow]"

# Install with docs tooling
pip install -e ".[dev,docs]"
```

## Running Tests

```bash
pytest -q
```

## Linting & Type Checking

```bash
ruff check src/ tests/
mypy src/
```

## Verify Plugin Registration

After installing, confirm all six sources are visible to DataHub:

```bash
datahub check plugins
```

You should see `infa_pmrep`, `autosys_jil`, `oracle_operational`, `essbase`, `ssis_dtsx`, and `abinitio` in the output.

## Project Structure

```
src/datahub_custom_sources/
├── __init__.py              # Re-exports: run_recipe, run_recipe_dict, load_recipe
├── runner.py                # Programmatic API (YAML or dict → Pipeline)
├── cli.py                   # Typer CLI: `dhcs ingest -c recipe.yml`
├── config.py                # Pydantic v2 config models for all sources
├── airflow/
│   ├── __init__.py          # Re-exports DataHubIngestionOperator
│   └── operators.py         # Airflow BaseOperator (Jinja, XCom, dry-run)
├── emit/
│   └── builders.py          # Shared MCP builder helpers
├── extractors/              # Per-system parsers
│   ├── pmrep.py             # Informatica pmrep CLI wrapper
│   ├── informatica_xml.py   # Informatica folder XML parser
│   ├── autosys_jil.py       # AutoSys JIL text parser
│   ├── ssis_dtsx.py         # SSIS DTSX XML parser + SQL lineage
│   ├── abinitio.py          # Ab Initio DML/graph/XFR parser
│   ├── essbase_client.py    # Essbase REST API client
│   ├── essbase_schema.py    # Essbase dimension schema builder
│   └── essbase_models.py    # Essbase data models
├── sources/                 # DataHub Source plugins
│   ├── common.py            # SimpleReport, as_workunits()
│   ├── informatica.py       # InformaticaPmrepSource
│   ├── autosys.py           # AutoSysJilSource
│   ├── oracle_operational.py # OracleOperationalSource
│   ├── essbase.py           # EssbaseSource
│   ├── ssis.py              # SsisDtsxSource
│   └── abinitio.py          # AbInitioSource
├── operational/
│   └── oracle_runner.py     # Oracle DataProcessInstance runner
└── utils/
    ├── urns.py              # URN construction helpers
    ├── paths.py             # expand_paths() shared utility
    └── subprocess.py        # Shell command runner
```

## Adding a New Source Plugin

1. **Config** — Add a Pydantic `BaseModel` in [config.py](https://github.com/beauschwab/datahub-infa-autosys/blob/main/src/datahub_custom_sources/config.py)
2. **Extractor** — Create a parser in `extractors/` for the source system's artifacts
3. **Source** — Create a Source class in `sources/` implementing the DataHub protocol:
   ```python
   class MySource:
       @classmethod
       def create(cls, config_dict, ctx): ...
       def get_workunits(self) -> Iterable[MetadataWorkUnit]: ...
       def get_report(self) -> SimpleReport: ...
       def close(self) -> None: ...
   ```
4. **Paths** — Use `expand_paths()` from `utils/paths.py` for any directory-scanning config
5. **MCPs** — Use builders from `emit/builders.py` and URN helpers from `utils/urns.py`
6. **Entry point** — Register in `pyproject.toml` under `[project.entry-points."datahub.ingestion.source.plugins"]`
7. **Recipe** — Add a sample recipe in `examples/recipes/`
8. **Export** — Add the Source class to `sources/__init__.py`

## Conventions

- **Logging**: Use `logging.getLogger(__name__)` — no Rich console in library code
- **Configs**: Pydantic v2 `BaseModel` with `model_validate()`
- **URNs**: Always use helpers from `utils/urns.py`
- **Directory scanning**: Always use `expand_paths()` from `utils/paths.py`
- **SQL truncation**: Hash long SQL with SHA-256 and store as `sql_snippets_sha256` custom property
- **Passwords**: Always read from environment variables via `password_env` fields

## Building Docs

```bash
pip install -e ".[docs]"
mkdocs serve       # local preview at http://127.0.0.1:8000
mkdocs build       # build static site to site/
```

## CI / Release

The project uses:

- **ruff** for linting and formatting
- **mypy** for type checking
- **pytest** for testing

A typical CI pipeline:

```bash
pip install -e ".[dev]"
ruff check src/ tests/
mypy src/
pytest -q
```
