# Kubernetes Deployment Guide

This directory contains reference configurations for deploying the DataHub metadata ingestion pipeline in a Kubernetes environment using Apache Airflow.

## Overview

The k8s reference DAG (`k8s_metadata_ingestion_dag.py`) is production-ready and includes:

- ✅ **KubernetesExecutor** configuration with pod-level resource controls
- ✅ **ConfigMap** integration for recipe YAML files
- ✅ **Secret** management for credentials (Informatica, Oracle, Essbase)
- ✅ **Resource limits** and requests per task (CPU, memory)
- ✅ **Error handling** with configurable retries and exponential backoff
- ✅ **Monitoring** via task logs and XCom outputs

## Quick Start

### 1. Install the Package

Build and install `datahub-custom-sources` in your Airflow Docker image:

```dockerfile
# In your Airflow Dockerfile
FROM apache/airflow:3.0.0-python3.11

# Install the package
RUN pip install --no-cache-dir "datahub-custom-sources[airflow]"
```

Or install from a private PyPI server:

```dockerfile
RUN pip install --no-cache-dir \
    --index-url https://pypi.company.internal/simple \
    "datahub-custom-sources[airflow]"
```

### 2. Create ConfigMap for Recipes

Apply the ConfigMap containing all six recipe files:

```bash
kubectl apply -f configmap-recipes.yaml -n airflow
```

Or create from the examples directory:

```bash
kubectl create configmap datahub-recipes \
  --from-file=../recipes/ \
  -n airflow
```

**Verify:**
```bash
kubectl get configmap datahub-recipes -n airflow -o yaml
```

### 3. Create Secret for Credentials

Create the Secret with your actual credentials:

```bash
kubectl create secret generic datahub-credentials \
  --from-literal=INFA_PASSWORD='your_informatica_password' \
  --from-literal=ORACLE_PASSWORD='your_oracle_password' \
  --from-literal=ESSBASE_PASSWORD='your_essbase_password' \
  -n airflow
```

**Verify:**
```bash
kubectl get secret datahub-credentials -n airflow
```

### 4. Configure Airflow Variables

Set these Airflow Variables via the UI or CLI:

```bash
airflow variables set DATAHUB_GMS_URL "http://datahub-gms:8080"
airflow variables set RECIPES_MOUNT_PATH "/opt/datahub/recipes"
```

Or via environment variables in your Airflow deployment:

```yaml
env:
  - name: AIRFLOW__CORE__LOAD_EXAMPLES
    value: "False"
  - name: AIRFLOW_VAR_DATAHUB_GMS_URL
    value: "http://datahub-gms:8080"
  - name: AIRFLOW_VAR_RECIPES_MOUNT_PATH
    value: "/opt/datahub/recipes"
```

### 5. Deploy the DAG

Copy the DAG file to your Airflow DAGs directory:

**Option A: GitSync (recommended)**

Configure Airflow to sync DAGs from Git:

```yaml
# In your Airflow Helm values.yaml
dags:
  gitSync:
    enabled: true
    repo: https://github.com/beauschwab/datahub-infa-autosys.git
    branch: main
    subPath: examples/airflow
```

**Option B: ConfigMap**

```bash
kubectl create configmap airflow-dags \
  --from-file=../airflow/k8s_metadata_ingestion_dag.py \
  -n airflow
```

**Option C: Bake into Docker image**

```dockerfile
COPY examples/airflow/k8s_metadata_ingestion_dag.py /opt/airflow/dags/
```

### 6. Verify and Trigger

The DAG should appear in the Airflow UI within a few minutes. Verify and trigger:

1. Open Airflow UI
2. Find DAG: `k8s_datahub_metadata_ingestion`
3. Check for parsing errors
4. Trigger manually or wait for schedule (daily 6 AM UTC)

## Architecture

### Resource Allocation

Each task runs in its own Kubernetes pod with:

- **Requests**: 1 CPU core, 2 GiB memory
- **Limits**: 2 CPU cores, 4 GiB memory

For sources requiring more resources, override `executor_config` per-task (see DAG comments).

### Task Dependencies

```
┌──────────────────┐
│ Informatica      │
│ (ingest_informatica) │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ AutoSys          │
│ (ingest_autosys) │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐          ┌──────────────────┐
│ Oracle Ops       │          │ Essbase          │
│ (ingest_oracle_  │          │ (ingest_essbase) │
│  operational)    │          └──────────────────┘
└──────────────────┘
                              ┌──────────────────┐
                              │ SSIS             │
                              │ (ingest_ssis)    │
                              └──────────────────┘

                              ┌──────────────────┐
                              │ Ab Initio        │
                              │ (ingest_abinitio)│
                              └──────────────────┘
```

- **Sequential**: Informatica → AutoSys → Oracle Ops
- **Parallel**: Essbase, SSIS, Ab Initio run independently

### Volume Mounts

| Volume | Mount Path | Source | Mode |
|--------|------------|--------|------|
| `recipes` | `/opt/datahub/recipes` | ConfigMap `datahub-recipes` | ReadOnly |

### Environment Variables

| Variable | Source | Usage |
|----------|--------|-------|
| `INFA_PASSWORD` | Secret `datahub-credentials` | Informatica pmrep authentication |
| `ORACLE_PASSWORD` | Secret `datahub-credentials` | Oracle DB connection |
| `ESSBASE_PASSWORD` | Secret `datahub-credentials` | Essbase REST API authentication |

## Customization

### Adjusting Resources

For large-scale ingestion, increase per-task resources:

```python
ingest_large_source = DataHubIngestionOperator(
    task_id="ingest_large_source",
    recipe_path=f"{RECIPES_DIR}/large_source.yml",
    executor_config={
        "pod_override": {
            "spec": {
                "containers": [{
                    "name": "base",
                    "resources": {
                        "requests": {"memory": "8Gi", "cpu": "4000m"},
                        "limits": {"memory": "16Gi", "cpu": "8000m"},
                    }
                }]
            }
        }
    },
)
```

### Node Affinity

To constrain pods to specific nodes:

```python
EXECUTOR_CONFIG = {
    "pod_override": {
        "spec": {
            "nodeSelector": {
                "workload": "data-platform"
            },
            "tolerations": [
                {
                    "key": "data-platform",
                    "operator": "Equal",
                    "value": "true",
                    "effect": "NoSchedule"
                }
            ],
        }
    }
}
```

### Changing DataHub Endpoint

Update the Airflow Variable:

```bash
airflow variables set DATAHUB_GMS_URL "http://datahub-gms.production.svc.cluster.local:8080"
```

Or modify the ConfigMap recipes to point to a different sink.

## Troubleshooting

### DAG Not Appearing

1. Check scheduler logs:
   ```bash
   kubectl logs -n airflow deployment/airflow-scheduler
   ```

2. Verify DAG file syntax:
   ```bash
   python examples/airflow/k8s_metadata_ingestion_dag.py
   ```

3. Check DAG import errors in Airflow UI: **Admin → Audit Log**

### Task Failures

1. View task logs in Airflow UI or via kubectl:
   ```bash
   kubectl logs -n airflow -l airflow-task=ingest_informatica
   ```

2. Check XCom output for errors:
   ```python
   # In Airflow UI: Admin → XComs
   # Key: datahub_report
   # Look for "failures" list
   ```

3. Verify ConfigMap and Secret are mounted:
   ```bash
   kubectl describe pod -n airflow <task-pod-name>
   ```

### Permission Denied Errors

Ensure the Secret exists and task pods have proper RBAC:

```bash
kubectl get secret datahub-credentials -n airflow
kubectl describe serviceaccount airflow-worker -n airflow
```

### Resource Quota Exceeded

If tasks fail with "Insufficient memory/cpu":

```bash
kubectl describe resourcequota -n airflow
```

Adjust task resource requests or increase namespace quota.

## Security Best Practices

1. **Use External Secrets Operator** for credential management:
   - AWS Secrets Manager
   - HashiCorp Vault
   - Google Secret Manager

2. **Enable RBAC** to restrict access to Secrets and ConfigMaps

3. **Use Network Policies** to limit pod-to-pod communication

4. **Rotate credentials** regularly

5. **Audit access** via Kubernetes audit logs

## Advanced: CeleryExecutor with KubernetesPodOperator

For mixed workloads (some tasks on Celery, some on K8s pods):

```python
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

ingest_heavyweight = KubernetesPodOperator(
    task_id="ingest_heavyweight",
    name="datahub-ingest-heavyweight",
    namespace="airflow",
    image="your-registry/datahub-ingestion:latest",
    cmds=["dhcs"],
    arguments=["ingest", "-c", "/opt/recipes/heavy_source.yml"],
    config_maps=["datahub-recipes"],
    secrets=[Secret("env", "INFA_PASSWORD", "datahub-credentials", "INFA_PASSWORD")],
    resources={
        "request_memory": "16Gi",
        "request_cpu": "8000m",
        "limit_memory": "32Gi",
        "limit_cpu": "16000m",
    },
)
```

## Support

For issues, questions, or contributions:

- **GitHub Issues**: https://github.com/beauschwab/datahub-infa-autosys/issues
- **Documentation**: https://github.com/beauschwab/datahub-infa-autosys/blob/main/docs/airflow.md
- **Examples**: https://github.com/beauschwab/datahub-infa-autosys/tree/main/examples
