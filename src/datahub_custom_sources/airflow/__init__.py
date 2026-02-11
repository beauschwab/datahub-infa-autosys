"""
Airflow integration for DataHub ingestion recipes.

Provides a reusable operator that can be used in any Airflow DAG to schedule
metadata ingestion from Informatica, AutoSys, Essbase, SSIS, Ab Initio, or
Oracle operational lineage.

Example DAG::

    from datetime import datetime
    from airflow.sdk import DAG
    from datahub_custom_sources.airflow import DataHubIngestionOperator

    with DAG("metadata_ingestion", start_date=datetime(2025, 1, 1), schedule="@daily") as dag:
        ingest_infa = DataHubIngestionOperator(
            task_id="ingest_informatica",
            recipe_path="/opt/recipes/infa_pmrep.yml",
        )
        ingest_autosys = DataHubIngestionOperator(
            task_id="ingest_autosys",
            recipe_path="/opt/recipes/autosys_jil.yml",
        )
        ingest_infa >> ingest_autosys
"""

__all__ = ["DataHubIngestionOperator"]

from datahub_custom_sources.airflow.operators import DataHubIngestionOperator
