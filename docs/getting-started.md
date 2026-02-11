# Getting Started

## Prerequisites

- **Python 3.10+**
- A running **DataHub** instance (GMS REST endpoint)
- **DataHub CLI** installed: `pip install acryl-datahub`

## Installation

=== "Core only"

    ```bash
    pip install datahub-custom-sources
    ```

=== "With Airflow support"

    ```bash
    pip install "datahub-custom-sources[airflow]"
    ```

=== "From source (development)"

    ```bash
    git clone https://github.com/beauschwab/datahub-custom-sources.git
    cd datahub-custom-sources
    python -m venv .venv
    source .venv/bin/activate   # Linux/macOS
    # .venv\Scripts\activate    # Windows
    pip install -e ".[dev]"
    ```

## Verify Installation

After installing, confirm the plugins are registered with DataHub:

```bash
datahub check plugins
```

You should see entries for `infa_pmrep`, `autosys_jil`, `oracle_operational`, `essbase`, `ssis_dtsx`, and `abinitio`.

## Quick Start

### 1. Create a recipe

Create a YAML recipe file that specifies the source, its configuration, and the DataHub sink. Here's a minimal example for AutoSys:

```yaml
# my_recipe.yml
source:
  type: autosys_jil
  config:
    jil_paths:
      - /data/export/autosys.jil
    env: PROD

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

See the [Recipes](recipes.md) page for all source examples.

### 2. Run the recipe

=== "DataHub CLI"

    ```bash
    datahub ingest -c my_recipe.yml
    ```

=== "Project CLI (with dry-run)"

    ```bash
    dhcs ingest -c my_recipe.yml --dry-run
    ```

=== "Python"

    ```python
    from datahub_custom_sources import run_recipe
    run_recipe("my_recipe.yml")
    ```

### 3. View in DataHub

Navigate to your DataHub UI. You should see the emitted DataFlows, DataJobs, and datasets with lineage edges.

## What's Next

- **[Sources](sources/index.md)** — Detailed configuration for each of the six source plugins
- **[Recipes](recipes.md)** — Complete recipe examples for every source
- **[Airflow Integration](airflow.md)** — Schedule ingestion with Airflow 3.x
- **[Python API](api-reference.md)** — Use the programmatic runner in scripts and notebooks
