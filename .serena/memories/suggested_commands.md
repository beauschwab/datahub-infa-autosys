Setup:
- python -m venv .venv
- pip install -U pip
- pip install -e " .[dev]"

Tests:
- pytest -q

DataHub ingestion (examples):
- datahub ingest -c examples/recipes/infa_pmrep.yml
- datahub ingest -c examples/recipes/autosys_jil.yml
- datahub ingest -c examples/recipes/oracle_operational.yml

CLI:
- dhcs extract informatica --config examples/configs/infa.json
- dhcs extract autosys --config examples/configs/autosys.json
- dhcs ingest --config examples/configs/combined.json
