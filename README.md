# datahub-infa-autosys

Custom **DataHub** ingestion sources (plugins) that model **nested AutoSys orchestration** running **Informatica workflows** (extracted via `pmrep`) and optionally emit **Oracle operational lineage** via `DataProcessInstance` runs.

## What you get

- `infa_pmrep` source: exports/parses Informatica objects using `pmrep` + XML parsing, emits:
  - `DataFlow` (Informatica folder/project)
  - `DataJob` (workflow / session / mapping, configurable granularity)
  - dataset inputs/outputs for each job
  - SQL snippets & stored-procedure calls captured as transformation text + optional SQL-lineage inference

- `autosys_jil` source: parses AutoSys JIL (boxes + jobs), emits:
  - `DataFlow` (box)
  - `DataJob` (job)
  - job→job dependencies (sequence + condition graph)
  - optional bridging edges from AutoSys job → Informatica workflow job (so nested orchestration is explicit)

- `oracle_operational` source: emits runtime facts as `DataProcessInstance`:
  - one instance per AutoSys run / Informatica run / Oracle proc run (configurable)
  - input/output datasets actually touched during that execution
  - run events (STARTED / COMPLETE + SUCCESS/FAILURE/etc)
  - edge properties for partition predicates, batch ids, run ids, etc.

> Note: Field/column-level lineage is emitted primarily using Dataset-level lineage aspects (DataHub UI support is strongest there). Job-level fine-grained lineage exists in the model but is not universally displayed by the UI; this package keeps the “truth” in dataset lineage and uses jobs for orchestration + explainability.

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install .
datahub check plugins
```

## Run via DataHub CLI

### 1) Informatica definitions

```yaml
# examples/recipes/infa_pmrep.yml
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
      include_workflows: true
      include_mappings: true
      out_dir: /tmp/infa_export
    lineage:
      platform: oracle
      platform_instance: PROD
      default_db: DW
      default_schema: 2052A
      ignore_table_patterns: ["DUMMY_%", "TEMP_%"]
      infer_from_sql: true

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

```bash
datahub ingest -c examples/recipes/infa_pmrep.yml
```

### 2) AutoSys orchestration

```bash
datahub ingest -c examples/recipes/autosys_jil.yml
```

### 3) Oracle operational lineage (runs)

```bash
datahub ingest -c examples/recipes/oracle_operational.yml
```

## Or run via the helper CLI (`dhia`)

The `dhia` CLI is there for local debugging and “extract → inspect → emit”.

```bash
dhia extract informatica --config examples/configs/infa.json
dhia extract autosys --config examples/configs/autosys.json
dhia ingest --config examples/configs/combined.json
```

## Development

```bash
pip install -e ".[dev]"
pytest -q
```
