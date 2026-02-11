Purpose: custom DataHub ingestion sources (plugins) for Informatica pmrep, AutoSys JIL orchestration, and Oracle operational lineage. Emits DataFlow/DataJob, dataset lineage, and DataProcessInstance runs.

Tech stack: Python 3.10+, DataHub SDK/acryl-datahub ingestion framework, Pydantic v2 models, Typer CLI, Rich, oracledb. Packaging via setuptools.

Codebase structure:
- src/datahub_custom_sources/sources: DataHub Source plugins (informatica, autosys, oracle_operational)
- src/datahub_custom_sources/extractors: pmrep and XML/JIL extractors
- src/datahub_custom_sources/emit: MCP builders
- src/datahub_custom_sources/utils: URN helpers, subprocess utils
- examples/recipes: DataHub ingest recipes
- docs: design notes (SSIS, abinitio, operational lineage, etc.)

Entry points:
- CLI: dhcs -> datahub_custom_sources.cli:app
- DataHub sources: infa_pmrep, autosys_jil, oracle_operational
