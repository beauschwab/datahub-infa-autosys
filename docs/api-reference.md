# Python API Reference

The `datahub_custom_sources` package exposes a programmatic API for running DataHub ingestion pipelines from Python code, Airflow DAGs, or any orchestrator.

## Top-Level Imports

```python
from datahub_custom_sources import run_recipe, run_recipe_dict, load_recipe
```

---

## `run_recipe`

Run a DataHub ingestion pipeline from a YAML recipe file.

```python
def run_recipe(
    recipe_path: str,
    dry_run: bool = False,
    report_to: str | None = None,
) -> Pipeline
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `recipe_path` | `str` | Path to a YAML recipe file |
| `dry_run` | `bool` | Validate and parse only — do not emit to the sink |
| `report_to` | `str?` | Path to write a JSON report file after completion |

**Returns:** A DataHub `Pipeline` instance (from `datahub.ingestion.run.pipeline`).

**Example:**

```python
from datahub_custom_sources import run_recipe

pipeline = run_recipe("examples/recipes/infa_pmrep.yml")
```

---

## `run_recipe_dict`

Run a DataHub ingestion pipeline from an in-memory recipe dict.

```python
def run_recipe_dict(
    recipe: dict,
    dry_run: bool = False,
    report_to: str | None = None,
) -> Pipeline
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `recipe` | `dict` | Recipe configuration as a Python dictionary |
| `dry_run` | `bool` | Validate and parse only — do not emit to the sink |
| `report_to` | `str?` | Path to write a JSON report file after completion |

**Returns:** A DataHub `Pipeline` instance.

**Example:**

```python
from datahub_custom_sources import run_recipe_dict

pipeline = run_recipe_dict({
    "source": {
        "type": "autosys_jil",
        "config": {"jil_paths": ["/data/autosys/dump.jil"]},
    },
    "sink": {
        "type": "datahub-rest",
        "config": {"server": "http://localhost:8080"},
    },
})
```

---

## `load_recipe`

Load a YAML recipe file into a Python dict without running it.

```python
def load_recipe(recipe_path: str) -> dict
```

Useful for inspecting, modifying, or merging recipes before execution.

**Example:**

```python
from datahub_custom_sources import load_recipe

recipe = load_recipe("examples/recipes/essbase.yml")
recipe["source"]["config"]["applications"] = ["FINANCE", "HR"]
```

---

## Airflow Operator

```python
from datahub_custom_sources.airflow import DataHubIngestionOperator
```

### `DataHubIngestionOperator`

An Airflow `BaseOperator` subclass (Airflow 3.x SDK) that runs a DataHub ingestion pipeline.

```python
class DataHubIngestionOperator(BaseOperator):
    template_fields: Sequence[str] = ("recipe_path",)

    def __init__(
        self,
        *,
        recipe_path: str | None = None,
        recipe: dict | None = None,
        dry_run: bool = False,
        report_to: str | None = None,
        **kwargs,
    ) -> None: ...
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `recipe_path` | `str?` | Path to YAML recipe (Jinja-templated) |
| `recipe` | `dict?` | Inline recipe dict |
| `dry_run` | `bool` | Validate only |
| `report_to` | `str?` | JSON report output path |

**XCom output:** Pushes `datahub_report` key with `{workunits, warnings, failures}`.

See [Airflow Integration](airflow.md) for usage patterns.

---

## Source Classes

All source classes implement the DataHub `Source` protocol and are registered as `datahub.ingestion.source.plugins` entry points.

| Import Path | Class |
|-------------|-------|
| `datahub_custom_sources.sources.informatica` | `InformaticaPmrepSource` |
| `datahub_custom_sources.sources.autosys` | `AutoSysJilSource` |
| `datahub_custom_sources.sources.oracle_operational` | `OracleOperationalSource` |
| `datahub_custom_sources.sources.essbase` | `EssbaseSource` |
| `datahub_custom_sources.sources.ssis` | `SsisDtsxSource` |
| `datahub_custom_sources.sources.abinitio` | `AbInitioSource` |

Each source follows the standard DataHub lifecycle:

```python
source = MySource.create(config_dict, pipeline_ctx)
for workunit in source.get_workunits():
    # process workunit
report = source.get_report()
source.close()
```

---

## MCP Builders

Low-level helpers for constructing DataHub MetadataChangeProposal events:

```python
from datahub_custom_sources.emit.builders import (
    make_edge,
    mcp_dataflow_info,
    mcp_datajob_info,
    mcp_datajob_io,
    mcp_dataset_properties,
    mcp_dataset_platform_instance,
    mcp_schema_metadata,
    mcp_upstream_lineage,
)
```

These are used internally by all six sources but are also available for custom extensions.

---

## URN Helpers

```python
from datahub_custom_sources.utils.urns import (
    dataset_urn,
    dataflow_urn,
    datajob_urn,
)
```

---

## Path Utilities

```python
from datahub_custom_sources.utils.paths import expand_paths
```

De-duplicate and expand file paths + directory globs:

```python
paths = expand_paths(
    explicit_paths=["a.dtsx", "b.dtsx"],
    dirs=["/opt/ssis/packages"],
    extensions=[".dtsx"],
)
```
