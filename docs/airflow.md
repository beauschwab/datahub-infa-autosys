# Airflow Integration

Schedule metadata ingestion from any of the six supported systems using Apache Airflow 3.x.

## Installation

```bash
pip install "datahub-custom-sources[airflow]"
```

This installs the core package plus `apache-airflow>=3.0` as a dependency.

## DataHubIngestionOperator

The package ships a ready-to-use Airflow operator that wraps the DataHub ingestion pipeline.

```python
from datahub_custom_sources.airflow import DataHubIngestionOperator
```

### Constructor

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| **task_id** | `str` | *required* | Airflow task identifier |
| recipe_path | `str?` | `None` | Path to YAML recipe file (supports Jinja templating) |
| recipe | `dict?` | `None` | Inline recipe as a Python dict |
| dry_run | `bool` | `False` | Parse and validate without emitting |
| report_to | `str?` | `None` | Path to write JSON run report |

!!! note
    Provide **either** `recipe_path` or `recipe`, not both. `recipe_path` is a Jinja template field — it can reference Airflow variables and macros.

### XCom Output

On completion, the operator pushes an XCom with key `"datahub_report"` containing:

```json
{
  "workunits": 142,
  "warnings": [],
  "failures": []
}
```

## Usage Patterns

### Option 1 — YAML Recipe File

The simplest approach: point the operator at a recipe YAML file on disk.

```python
from airflow.sdk import DAG
from datahub_custom_sources.airflow import DataHubIngestionOperator
from datetime import datetime

with DAG("ingest_metadata", start_date=datetime(2025, 1, 1), schedule="@daily"):
    DataHubIngestionOperator(
        task_id="ingest_informatica",
        recipe_path="/opt/recipes/infa_pmrep.yml",
    )
```

### Option 2 — Inline Recipe Dict

Embed the recipe directly in the DAG for self-contained deployments.

```python
DataHubIngestionOperator(
    task_id="ingest_autosys",
    recipe={
        "source": {
            "type": "autosys_jil",
            "config": {
                "jil_paths": ["/data/autosys/full_dump.jil"],
            },
        },
        "sink": {
            "type": "datahub-rest",
            "config": {"server": "http://datahub:8080"},
        },
    },
)
```

### Option 3 — Jinja-Templated Path

Use Airflow variables or macros in the recipe path.

```python
DataHubIngestionOperator(
    task_id="ingest_ssis",
    recipe_path="/opt/recipes/{{ var.value.env }}/ssis_dtsx.yml",
)
```

## Programmatic API (Without the Operator)

If you prefer to use `PythonOperator` or `@task`:

```python
from airflow.sdk import dag, task
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2025, 1, 1))
def metadata_pipeline():

    @task
    def ingest_informatica():
        from datahub_custom_sources import run_recipe
        run_recipe("/opt/recipes/infa_pmrep.yml")

    @task
    def ingest_from_dict():
        from datahub_custom_sources import run_recipe_dict
        run_recipe_dict({
            "source": {"type": "essbase", "config": {"base_url": "https://essbase.company.com", ...}},
            "sink": {"type": "datahub-rest", "config": {"server": "http://datahub:8080"}},
        })

    ingest_informatica() >> ingest_from_dict()

metadata_pipeline()
```

## Example DAG

A complete multi-source DAG is included at [`examples/airflow/metadata_ingestion_dag.py`](https://github.com/beauschwab/datahub-infa-autosys/blob/main/examples/airflow/metadata_ingestion_dag.py):

```python
from airflow.sdk import DAG
from datahub_custom_sources.airflow import DataHubIngestionOperator
from datetime import datetime

RECIPES = "/opt/recipes"

with DAG("datahub_metadata_ingestion", start_date=datetime(2025, 1, 1), schedule="@daily"):
    infa = DataHubIngestionOperator(task_id="informatica", recipe_path=f"{RECIPES}/infa_pmrep.yml")
    auto = DataHubIngestionOperator(task_id="autosys", recipe_path=f"{RECIPES}/autosys_jil.yml")
    orcl = DataHubIngestionOperator(task_id="oracle_ops", recipe_path=f"{RECIPES}/oracle_operational.yml")
    essb = DataHubIngestionOperator(task_id="essbase", recipe_path=f"{RECIPES}/essbase.yml")
    ssis = DataHubIngestionOperator(task_id="ssis", recipe_path=f"{RECIPES}/ssis_dtsx.yml")
    abin = DataHubIngestionOperator(task_id="abinitio", recipe_path=f"{RECIPES}/abinitio.yml")

    infa >> auto >> orcl
```

## Airflow 3.x Compatibility

This package targets **Airflow 3.0+** and uses the modern SDK imports:

```python
from airflow.sdk import BaseOperator, Context, DAG
```

If you need to support Airflow 2.x, pin `datahub-custom-sources[airflow]` to a version that still uses `airflow.models.BaseOperator`.
