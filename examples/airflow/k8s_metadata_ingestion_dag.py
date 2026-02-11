"""
Reference Airflow DAG for Kubernetes: DataHub metadata ingestion pipeline.

This production-ready DAG is designed to run in a Kubernetes environment using
the KubernetesExecutor or KubernetesPodOperator pattern. It orchestrates metadata
extraction and ingestion for all six supported data sources.

Key Features
------------
* **K8s-native**: Configured for KubernetesExecutor with pod-level resources
* **ConfigMap integration**: Recipe YAML files mounted from ConfigMaps
* **Secret management**: Credentials via Kubernetes Secrets
* **Resource management**: CPU/memory limits and requests per task
* **Error handling**: Configurable retries with exponential backoff
* **Monitoring**: Task-level logging and XCom outputs for observability

Prerequisites
-------------
1. Install the package in your Airflow image::

       pip install "datahub-custom-sources[airflow]"

2. Create ConfigMaps for recipe files::

       kubectl create configmap datahub-recipes \
         --from-file=examples/recipes/ \
         -n airflow

3. Create Secrets for credentials::

       kubectl create secret generic datahub-credentials \
         --from-literal=INFA_PASSWORD=your_infa_password \
         --from-literal=ORACLE_PASSWORD=your_oracle_password \
         --from-literal=ESSBASE_PASSWORD=your_essbase_password \
         -n airflow

4. Configure Airflow Variables::

       DATAHUB_GMS_URL: http://datahub-gms:8080
       RECIPES_MOUNT_PATH: /opt/datahub/recipes

Deployment
----------
Place this file in your Airflow DAGs folder (e.g., via GitSync, ConfigMap, or
image bake-in). The DAG will appear in the Airflow UI and can be triggered
manually or run on schedule.

For detailed setup instructions, see:
https://github.com/beauschwab/datahub-infa-autosys/blob/main/docs/airflow.md
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.models import Variable
from datahub_custom_sources.airflow import DataHubIngestionOperator

# ───────────────────────────────────────────────────────────────────────────
# Configuration
# ───────────────────────────────────────────────────────────────────────────

# Recipe files are expected to be mounted from a ConfigMap at this path
RECIPES_DIR = Variable.get("RECIPES_MOUNT_PATH", default_var="/opt/datahub/recipes")

# DataHub GMS endpoint (typically a Kubernetes service)
DATAHUB_GMS_URL = Variable.get("DATAHUB_GMS_URL", default_var="http://datahub-gms:8080")

# Kubernetes executor configuration for each task pod
EXECUTOR_CONFIG = {
    "pod_override": {
        "metadata": {
            "annotations": {
                "cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
            }
        },
        "spec": {
            "containers": [
                {
                    "name": "base",
                    "resources": {
                        "requests": {
                            "memory": "2Gi",
                            "cpu": "1000m",
                        },
                        "limits": {
                            "memory": "4Gi",
                            "cpu": "2000m",
                        },
                    },
                    "env": [
                        {
                            "name": "INFA_PASSWORD",
                            "valueFrom": {
                                "secretKeyRef": {
                                    "name": "datahub-credentials",
                                    "key": "INFA_PASSWORD",
                                }
                            },
                        },
                        {
                            "name": "ORACLE_PASSWORD",
                            "valueFrom": {
                                "secretKeyRef": {
                                    "name": "datahub-credentials",
                                    "key": "ORACLE_PASSWORD",
                                }
                            },
                        },
                        {
                            "name": "ESSBASE_PASSWORD",
                            "valueFrom": {
                                "secretKeyRef": {
                                    "name": "datahub-credentials",
                                    "key": "ESSBASE_PASSWORD",
                                }
                            },
                        },
                    ],
                    "volumeMounts": [
                        {
                            "name": "recipes",
                            "mountPath": RECIPES_DIR,
                            "readOnly": True,
                        },
                    ],
                }
            ],
            "volumes": [
                {
                    "name": "recipes",
                    "configMap": {
                        "name": "datahub-recipes",
                    },
                }
            ],
            "restartPolicy": "Never",
            # Optionally constrain to specific node pools
            # "nodeSelector": {
            #     "workload": "data-platform"
            # },
            # "tolerations": [
            #     {
            #         "key": "data-platform",
            #         "operator": "Equal",
            #         "value": "true",
            #         "effect": "NoSchedule"
            #     }
            # ],
        },
    }
}

# DAG default arguments
default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email": ["data-platform@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(hours=1),
    "execution_timeout": timedelta(hours=2),
    "executor_config": EXECUTOR_CONFIG,
}

# ───────────────────────────────────────────────────────────────────────────
# DAG Definition
# ───────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="k8s_datahub_metadata_ingestion",
    description="Production metadata ingestion for all DataHub sources in Kubernetes",
    default_args=default_args,
    schedule="0 6 * * *",  # Daily at 6 AM UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["datahub", "metadata", "lineage", "kubernetes", "production"],
) as dag:

    # ── Informatica (pmrep XML exports) ────────────────────────────────────
    # Extracts workflows, sessions, mappings, and SQL lineage from Informatica
    ingest_informatica = DataHubIngestionOperator(
        task_id="ingest_informatica",
        recipe_path=f"{RECIPES_DIR}/infa_pmrep.yml",
        doc_md="""
        ### Informatica Metadata Ingestion
        
        Extracts metadata from Informatica PowerCenter via pmrep CLI:
        - Workflows, sessions, mappings
        - Source/target table lineage
        - SQL snippets from transformations
        
        **Prerequisites**: 
        - `pmrep` binary available in container
        - INFA_PASSWORD secret configured
        """,
    )

    # ── AutoSys (JIL orchestration) ────────────────────────────────────────
    # Parses AutoSys JIL files for job definitions and dependencies
    ingest_autosys = DataHubIngestionOperator(
        task_id="ingest_autosys",
        recipe_path=f"{RECIPES_DIR}/autosys_jil.yml",
        doc_md="""
        ### AutoSys Metadata Ingestion
        
        Parses AutoSys JIL files to extract:
        - Job definitions (cmd, machine, owner)
        - Box hierarchies
        - Inter-job dependencies (condition statements)
        - Optional bridging to Informatica workflows
        
        **Prerequisites**: 
        - JIL export files available
        """,
    )

    # ── Oracle Operational Lineage ─────────────────────────────────────────
    # Streams runtime lineage events from Oracle audit tables
    ingest_oracle_ops = DataHubIngestionOperator(
        task_id="ingest_oracle_operational",
        recipe_path=f"{RECIPES_DIR}/oracle_operational.yml",
        doc_md="""
        ### Oracle Operational Lineage
        
        Streams DataProcessInstance events from Oracle audit tables:
        - Job execution events (STARTED, COMPLETE)
        - Input/output datasets per run
        - Partition predicates and batch IDs
        
        **Prerequisites**: 
        - Oracle DB connectivity
        - ORACLE_PASSWORD secret configured
        - Audit tables populated by operational processes
        """,
    )

    # ── Essbase (cubes + calc scripts + load rules) ────────────────────────
    # Fetches Essbase metadata via REST API
    ingest_essbase = DataHubIngestionOperator(
        task_id="ingest_essbase",
        recipe_path=f"{RECIPES_DIR}/essbase.yml",
        doc_md="""
        ### Essbase Metadata Ingestion
        
        Connects to Oracle Essbase via REST API:
        - Cube definitions and dimensions
        - Calc scripts (transformations)
        - Load rules (data sources)
        
        **Prerequisites**: 
        - Essbase REST API accessible
        - ESSBASE_PASSWORD secret configured
        """,
    )

    # ── SSIS (DTSX packages) ───────────────────────────────────────────────
    # Parses SQL Server Integration Services package files
    ingest_ssis = DataHubIngestionOperator(
        task_id="ingest_ssis",
        recipe_path=f"{RECIPES_DIR}/ssis_dtsx.yml",
        doc_md="""
        ### SSIS Metadata Ingestion
        
        Parses SSIS .dtsx package files:
        - Data flow tasks
        - SQL statements and stored procedures
        - Dataset lineage (best-effort)
        - Optional column-level lineage from schema JSON
        
        **Prerequisites**: 
        - DTSX package files available
        """,
    )

    # ── Ab Initio (graphs + DML/XFR) ───────────────────────────────────────
    # Extracts Ab Initio graph and schema metadata
    ingest_abinitio = DataHubIngestionOperator(
        task_id="ingest_abinitio",
        recipe_path=f"{RECIPES_DIR}/abinitio.yml",
        doc_md="""
        ### Ab Initio Metadata Ingestion
        
        Parses Ab Initio artifacts:
        - Graph definitions (ETL flows)
        - DML schema files
        - XFR transform mappings
        - Optional graph I/O from JSON
        
        **Prerequisites**: 
        - Graph, DML, XFR files exported to filesystem
        """,
    )

    # ───────────────────────────────────────────────────────────────────────
    # Task Dependencies
    # ───────────────────────────────────────────────────────────────────────
    
    # Sequential: Structural metadata first, then operational lineage
    # Informatica and AutoSys are often related, so run Infa before AutoSys
    ingest_informatica >> ingest_autosys >> ingest_oracle_ops

    # Independent: These sources have no cross-dependencies
    [ingest_essbase, ingest_ssis, ingest_abinitio]


# ───────────────────────────────────────────────────────────────────────────
# Alternative: High-Resource Tasks
# ───────────────────────────────────────────────────────────────────────────
#
# For sources requiring more resources, override executor_config per-task:
#
# ingest_large_source = DataHubIngestionOperator(
#     task_id="ingest_large_source",
#     recipe_path=f"{RECIPES_DIR}/large_source.yml",
#     executor_config={
#         "pod_override": {
#             "spec": {
#                 "containers": [{
#                     "name": "base",
#                     "resources": {
#                         "requests": {"memory": "8Gi", "cpu": "4000m"},
#                         "limits": {"memory": "16Gi", "cpu": "8000m"},
#                     }
#                 }]
#             }
#         }
#     },
# )
