# Ab Initio

**Plugin:** `abinitio` · **Source class:** `AbInitioSource`

Parses Ab Initio DML schema files, graph files, and XFR transform files to emit DataFlow/DataJob/dataset entities with schema metadata and lineage.

## How It Works

```
DML files → parse schema → Dataset with SchemaMetadata
Graph files (.mp/.g/.xml) → DataFlow (project) → DataJob (per graph)
XFR files → field-level transform mapping → UpstreamLineage
IO mapping JSON → graph input/output datasets → DataJob IO + lineage
```

1. Reads DML files from explicit paths and/or directories (`dml_dirs`) — emits datasets with schema
2. Reads graph files from explicit paths and/or directories (`graph_dirs`) — emits DataJobs
3. If XFR files are provided, parses field-level transform mappings for lineage
4. If `io_mapping_paths` are provided, wires input/output datasets to each graph's DataJob

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **graph_paths** | `list[str]` | *required* | Paths to Ab Initio graph files (`.mp`, `.g`, `.xml`) |
| graph_dirs | `list[str]` | `[]` | Directories to scan recursively for graph files |
| dml_paths | `list[str]` | `[]` | Paths to DML schema files |
| dml_dirs | `list[str]` | `[]` | Directories to scan recursively for DML files |
| xfr_paths | `list[str]` | `[]` | Paths to XFR transform files |
| xfr_dirs | `list[str]` | `[]` | Directories to scan for XFR files |
| io_mapping_paths | `list[str]` | `[]` | JSON files mapping graph names to input/output datasets |
| project_name | `str?` | `null` | Ab Initio project name for DataFlow IDs |
| dataflow_platform | `str` | `"abinitio"` | Platform for DataFlow URNs |
| dataflow_env | `str` | `"PROD"` | Environment for DataFlow URNs |
| emit_schema_from_dml | `bool` | `true` | Emit dataset schema from DML files |
| lineage.platform | `str` | `"oracle"` | Platform for dataset URNs |
| lineage.env | `str` | `"PROD"` | Environment for datasets |

### Example Recipe

```yaml
source:
  type: abinitio
  config:
    graph_paths:
      - "C:/abinitio/graphs/load_customer.mp"
    graph_dirs:
      - "C:/abinitio/graphs"
    dml_paths:
      - "C:/abinitio/dml/customer.dml"
      - "C:/abinitio/dml/customer_stg.dml"
    dml_dirs:
      - "C:/abinitio/dml"
    xfr_paths:
      - "C:/abinitio/xfr/customer_map.xfr"
    xfr_dirs:
      - "C:/abinitio/xfr"
    io_mapping_paths:
      - "C:/abinitio/io_mapping.json"
    project_name: "CustomerProject"
    dataflow_platform: "abinitio"
    dataflow_env: "PROD"
    lineage:
      platform: "hdfs"
      env: "PROD"

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

## Directory Scanning

Use `*_dirs` fields to automatically discover files in directory trees:

```yaml
config:
  graph_paths: []       # can be empty if using dirs
  graph_dirs:
    - "/opt/abinitio/graphs"
  dml_dirs:
    - "/opt/abinitio/dml"
  xfr_dirs:
    - "/opt/abinitio/xfr"
```

All file types are de-duplicated using the shared `expand_paths()` utility. Graph directories are scanned for `.mp`, `.g`, and `.xml` extensions; DML directories for `.dml`; XFR directories for `.xfr`.

## IO Mapping

The `io_mapping_paths` option accepts JSON files that map graph names to their input/output datasets. This enables the source to emit DataJob IO and dataset-level lineage:

```json
{
  "load_customer": {
    "inputs": ["raw.customer", "raw.customer_address"],
    "outputs": ["curated.customer"]
  },
  "load_orders": {
    "inputs": ["raw.orders", "raw.order_items"],
    "outputs": ["curated.orders"]
  }
}
```

When IO mapping is provided, the source creates:

- `mcp_datajob_io()` aspects linking each graph's DataJob to its input/output datasets
- `mcp_upstream_lineage()` aspects on output datasets pointing to their inputs

## Emitted Entities

| Entity | URN Pattern | Notes |
|--------|-------------|-------|
| DataFlow | `urn:li:dataFlow:(abinitio,PROJECT,PROD)` | One per project |
| DataJob | `urn:li:dataJob:(...,GRAPH_NAME)` | One per graph file |
| Dataset | `urn:li:dataset:(urn:li:dataPlatform:hdfs,DATASET_NAME,PROD)` | From DML + IO mapping |

## Further Reading

See [abinitio.md](../abinitio.md) for detailed design notes on Ab Initio artifact formats, DML syntax, and parsing strategies.
