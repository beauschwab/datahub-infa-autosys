"""
Example Airflow DAG: schedule DataHub metadata ingestion for all six sources.

Install into your Airflow environment as:

    pip install "/path/to/datahub-custom-sources[airflow]"

Then place this file under $AIRFLOW_HOME/dags/ and adjust the recipe paths.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG
from datahub_custom_sources.airflow import DataHubIngestionOperator

RECIPES_DIR = "/opt/datahub/recipes"

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="datahub_metadata_ingestion",
    description="Scheduled DataHub metadata ingestion from Informatica, AutoSys, "
                "Essbase, SSIS, Ab Initio, and Oracle operational lineage",
    default_args=default_args,
    schedule="0 6 * * *",  # 6 AM daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["datahub", "metadata", "lineage"],
) as dag:

    # â”€â”€ Informatica (pmrep XML exports) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ingest_informatica = DataHubIngestionOperator(
        task_id="ingest_informatica",
        recipe_path=f"{RECIPES_DIR}/infa_pmrep.yml",
    )

    # â”€â”€ AutoSys (JIL orchestration) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ingest_autosys = DataHubIngestionOperator(
        task_id="ingest_autosys",
        recipe_path=f"{RECIPES_DIR}/autosys_jil.yml",
    )

    # â”€â”€ Oracle operational lineage (run instances) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ingest_oracle_ops = DataHubIngestionOperator(
        task_id="ingest_oracle_operational",
        recipe_path=f"{RECIPES_DIR}/oracle_operational.yml",
    )

    # â”€â”€ Essbase (cubes + calc scripts + load rules) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ingest_essbase = DataHubIngestionOperator(
        task_id="ingest_essbase",
        recipe_path=f"{RECIPES_DIR}/essbase.yml",
    )

    # â”€â”€ SSIS (DTSX packages) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ingest_ssis = DataHubIngestionOperator(
        task_id="ingest_ssis",
        recipe_path=f"{RECIPES_DIR}/ssis_dtsx.yml",
    )

    # â”€â”€ Ab Initio (graphs + DML/XFR) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ingest_abinitio = DataHubIngestionOperator(
        task_id="ingest_abinitio",
        recipe_path=f"{RECIPES_DIR}/abinitio.yml",
    )

    # â”€â”€ Dependencies â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # Structural sources first â†’ operational lineage runs after.
    # Informatica & AutoSys are often related so run Infa before AutoSys.
    ingest_informatica >> ingest_autosys >> ingest_oracle_ops

    # The rest are independent of each other.
    [ingest_essbase, ingest_ssis, ingest_abinitio]


# ---------------------------------------------------------------------------
# Alternative: inline recipe (no YAML file needed)
# ---------------------------------------------------------------------------
# DataHubIngestionOperator(
#     task_id="ingest_inline",
#     recipe={
#         "source": {
#             "type": "autosys_jil",
#             "config": {
#                 "jil_paths": ["/data/export/autosys.jil"],
#                 "dataflow_platform": "autosys",
#                 "dataflow_env": "PROD",
#             },
#         },
#         "sink": {
#             "type": "datahub-rest",
#             "config": {"server": "http://datahub-gms:8080"},
#         },
#     },
# )
