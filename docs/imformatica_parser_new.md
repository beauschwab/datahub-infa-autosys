# Informatica PowerCenter to DataHub Lineage Integration

A comprehensive Python module for extracting column-level lineage from Informatica PowerCenter XML exports and emitting it to DataHub.

## Features

- **Complete XML Parsing**: Parses Informatica PowerCenter export XMLs including sources, targets, mappings, sessions, and workflows
- **Ordered Transformation Chains**: Captures the full transformation path (Source → SQ → Expression → Aggregator → Target) with step ordering
- **SQL Override Parsing**: Uses sqlglot to parse embedded SQL in Source Qualifier transformations for accurate column-level lineage
- **Stored Procedure References**: Handles Oracle stored procedure calls as cross-platform entity references
- **DataHub Integration**: Native Python SDK emission using `FineGrainedLineage`, `DataFlow`, and `DataJob` entities

## Installation

```bash
pip install -r requirements.txt
```

## Quick Start

```python
from informatica_lineage import InformaticaLineageExtractor

# Initialize extractor
extractor = InformaticaLineageExtractor(
    datahub_server="http://localhost:8080",
    platform="oracle",
    platform_instance="prod",
    oracle_platform_instance="prod-oracle",
    env="PROD",
)

# Process XML export
extractor.process_xml_file("workflow_export.xml")

# Get summary
summary = extractor.get_lineage_summary()
print(f"Extracted {summary['total_edges']} lineage edges")

# Emit to DataHub
extractor.emit_to_datahub()
```

## CLI Usage

```bash
python informatica_lineage.py workflow_export.xml \
    --datahub-server http://localhost:8080 \
    --platform oracle \
    --platform-instance prod \
    --env PROD
```

### CLI Options

| Option | Default | Description |
|--------|---------|-------------|
| `--datahub-server` | `http://localhost:8080` | DataHub GMS server URL |
| `--token` | None | DataHub authentication token |
| `--platform` | `oracle` | Database platform |
| `--platform-instance` | None | Platform instance identifier |
| `--oracle-platform-instance` | None | Separate instance for Oracle stored procedures |
| `--env` | `PROD` | Environment (PROD, DEV, etc.) |
| `--emit-intermediate-jobs` | True | Emit individual transformation DataJobs |
| `--use-dataset-for-procs` | False | Reference stored procedures as Datasets instead of DataJobs |
| `--dry-run` | False | Parse without emitting to DataHub |
| `--verbose` | False | Enable verbose logging |

## Architecture

### DataHub Entity Model

```
DataFlow (Workflow)
├── DataJob (Session)
│   ├── inputDatasets: [source tables]
│   ├── outputDatasets: [target tables]
│   ├── inputDatajobs: [stored procedure references]
│   └── fineGrainedLineages: [column-level mappings]
│
└── DataJob (Transformation) - optional intermediate jobs
    ├── SQ_CustomerOrders
    ├── EXP_Transform
    └── AGG_Revenue
```

### Transformation Type Handling

| Informatica Type | DataHub Handling |
|------------------|------------------|
| Source Definition | Dataset (input) |
| Source Qualifier | DataJob + SQL parsing for column lineage |
| Expression | DataJob with expression in `transformOperation` |
| Aggregator | DataJob with aggregation expression |
| Filter | DataJob with filter condition |
| Joiner | DataJob with join condition |
| Lookup | DataJob with lookup condition |
| Stored Procedure | Cross-reference to Oracle DataJob/Dataset |
| Target Definition | Dataset (output) with `UpstreamLineage` |

## Handling Special Cases

### Source Qualifier SQL Overrides

When a Source Qualifier has a custom SQL query, the module:
1. Extracts the SQL from `TABLEATTRIBUTE[@NAME='Sql Query']`
2. Parses it with sqlglot using Oracle dialect
3. Extracts column-level lineage including JOINs, aggregations, CASE statements
4. Emits `FineGrainedLineage` with the SQL expression

```xml
<TRANSFORMATION NAME="SQ_CUSTOMERS" TYPE="Source Qualifier">
    <TABLEATTRIBUTE NAME="Sql Query" VALUE="
        SELECT c.CUSTOMER_ID, c.FIRST_NAME, COUNT(o.ORDER_ID) AS ORDER_COUNT
        FROM CUSTOMERS c
        LEFT JOIN ORDERS o ON c.CUSTOMER_ID = o.CUSTOMER_ID
        GROUP BY c.CUSTOMER_ID, c.FIRST_NAME
    "/>
</TRANSFORMATION>
```

### Oracle Stored Procedure References

Stored procedure calls can be referenced two ways:

**As DataJobs (recommended for job-to-job lineage):**
```python
extractor = InformaticaLineageExtractor(
    use_dataset_for_procs=False,  # Default
    ...
)
# URN: urn:li:dataJob:(urn:li:dataFlow:(oracle,stored_procedures,PROD),PKG.PROC_NAME)
```

**As Datasets (matches Oracle connector default):**
```python
extractor = InformaticaLineageExtractor(
    use_dataset_for_procs=True,
    ...
)
# URN: urn:li:dataset:(urn:li:dataPlatform:oracle,PKG.PROC_NAME,PROD)
```

## Output Examples

### Lineage Summary
```
Informatica Lineage Extraction Summary
======================================
Total lineage edges: 24
Source datasets: 2
Target datasets: 1
Workflows processed: 1
Mappings processed: 1

Stored procedure references:
  - PKG_CUSTOMER.GET_OR_CREATE_KEY
  - DW.ENRICH_CUSTOMER_STATUS

Edges by transformation type:
  Source Qualifier: 7
  Expression: 12
  Stored Procedure: 3
  Sequence Generator: 2
```

### Column Lineage Chain
```
→ DW.DIM_CUSTOMERS.FULL_NAME
    ├─ [Expression] INITCAP(FIRST_NAME) || ' ' || UPPER(LAST_NAME)
    │     Inputs: [(SQ_CUSTOMER_ORDERS, FIRST_NAME), (SQ_CUSTOMER_ORDERS, LAST_NAME)]
    ├─ [Source Qualifier] SELECT ... FROM CUSTOMERS c ...
    │     Confidence: 0.95
    └─ [Source Definition] CRM.CUSTOMERS
```

## Module Components

### `InformaticaXMLParser`
Parses PowerCenter XML exports into Python dataclasses.

### `SQLLineageParser`
Extracts column-level lineage from SQL using sqlglot.

### `LineageBuilder`
Constructs ordered transformation chains and lineage edges.

### `DataHubLineageEmitter`
Builds and emits MCPs to DataHub using the Python SDK.

### `InformaticaLineageExtractor`
Main orchestrator combining all components.

## Extending the Module

### Adding New Transformation Types

```python
class TransformationType(Enum):
    # Add new type
    MY_CUSTOM_TRANSFORM = "My Custom Transform"

# Handle in LineageBuilder._process_transformation()
```

### Custom URN Formats

Override `_build_dataset_urn()` in `DataHubLineageEmitter` for custom URN construction.

## Limitations

- PL/SQL blocks in Source Qualifiers are not fully parsed (extract SELECT statements only)
- Reusable transformations across folders require folder context
- Parameter file resolution not implemented ($$PARAM references)
- No runtime execution context (static lineage only)

## License

MIT License
