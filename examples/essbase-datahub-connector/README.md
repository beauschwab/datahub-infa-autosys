# Essbase to DataHub Metadata Connector

Extract multidimensional schema, calculation logic, and fine-grained lineage from Oracle Essbase and emit to DataHub using the Python SDK.

## Features

- **Schema Extraction**: Full multidimensional schema including dimensions, members, hierarchies, and member properties
- **Calculation Script Parsing**: Parse calc scripts to extract FIX statements, formulas, AGG operations, DATACOPY, etc.
- **Fine-Grained Lineage**: Column-level lineage from:
  - Member formulas (dynamic calc members)
  - Consolidation paths (parent-child aggregation)
  - Calculation scripts (FIX scopes, transforms)
  - Load rules (source-to-dimension mappings)
- **OpenLineage-Compatible YAML**: Intermediate format for inspection, debugging, and interoperability
- **DataHub Integration**: Native emission via DataHub Python SDK with proper URN generation

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Essbase REST API                                  │
│  (Applications, Cubes, Dimensions, Members, Calc Scripts, Rules)    │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Extractors                                      │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐            │
│  │   Schema    │  │  CalcScript  │  │     Lineage     │            │
│  │  Extractor  │  │   Extractor  │  │    Extractor    │            │
│  └─────────────┘  └──────────────┘  └─────────────────┘            │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│               OpenLineage-Compatible Models (YAML)                   │
│  ┌───────────┐  ┌───────────┐  ┌───────────────────────┐           │
│  │  Datasets │  │   Jobs    │  │  Column Lineage       │           │
│  │  (Cubes)  │  │  (Calcs)  │  │  (Fine-Grained)       │           │
│  └───────────┘  └───────────┘  └───────────────────────┘           │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    DataHub Emitter                                   │
│  (Datasets, DataJobs, Schema, Lineage, Tags, Custom Properties)     │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         DataHub                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Installation

```bash
pip install essbase-datahub-connector
```

Or install from source:

```bash
git clone https://github.com/yourorg/essbase-datahub-connector.git
cd essbase-datahub-connector
pip install -e ".[dev]"
```

## Quick Start

### Python API

```python
from essbase_datahub import EssbaseDataHubConnector

# Initialize connector
connector = EssbaseDataHubConnector(
    essbase_url="https://essbase.company.com",
    essbase_user="admin",
    essbase_password="secret",
    datahub_server="http://datahub-gms:8080",
    datahub_token="your-token",  # Optional
)

# Sync a single cube
result = connector.sync_cube("FinanceApp", "Plan")
print(f"Synced {result['datasets']} datasets, {result['jobs']} jobs")

# Sync all cubes in an application  
result = connector.sync_application("FinanceApp")

# Sync everything
result = connector.sync_all()

# Extract to YAML only (no DataHub emission)
extraction = connector.extract_cube("FinanceApp", "Plan")
extraction.to_yaml("finance_plan_metadata.yaml")
```

### CLI

```bash
# Sync a single cube
essbase-datahub sync \
    --essbase-url https://essbase.company.com \
    --essbase-user admin \
    --essbase-password secret \
    --datahub-server http://datahub-gms:8080 \
    --app FinanceApp \
    --cube Plan

# Sync with YAML output (for inspection)
essbase-datahub sync \
    --essbase-url https://essbase.company.com \
    --essbase-user admin \
    --essbase-password secret \
    --datahub-server http://datahub-gms:8080 \
    --app FinanceApp \
    --output-dir ./extractions

# Dry run (extract only, don't emit)
essbase-datahub sync \
    --essbase-url https://essbase.company.com \
    --essbase-user admin \
    --essbase-password secret \
    --datahub-server http://datahub-gms:8080 \
    --app FinanceApp \
    --cube Plan \
    --dry-run

# Test connections
essbase-datahub test \
    --essbase-url https://essbase.company.com \
    --essbase-user admin \
    --essbase-password secret \
    --datahub-server http://datahub-gms:8080

# List applications
essbase-datahub list \
    --essbase-url https://essbase.company.com \
    --essbase-user admin \
    --essbase-password secret

# List cubes in an application
essbase-datahub list \
    --essbase-url https://essbase.company.com \
    --essbase-user admin \
    --essbase-password secret \
    --app FinanceApp
```

## What Gets Extracted

### Datasets (Cubes)

Each Essbase cube becomes a DataHub dataset with:

- **Schema Fields**: Dimensions and members as nested schema fields
- **Custom Properties**: Cube statistics, dimension storage types, etc.
- **Tags**: `essbase`, `multidimensional`, `olap`

### Schema Fields

Dimensions are represented as top-level fields with nested member fields:

```yaml
schema_fields:
  - name: Year
    type: dimension
    description: Time dimension with 12 members
    custom_properties:
      storage_type: sparse
      category: time
      member_count: "12"
    fields:
      - name: Q1
        type: member
        fields:
          - name: Jan
          - name: Feb
          - name: Mar
      - name: Q2
        ...
```

### Member Metadata

For each member, we capture:

- Consolidation operator (+, -, *, /, ~, ^)
- Storage type (store, dynamic_calc, shared, etc.)
- Member formulas
- User-defined attributes (UDAs)
- Aliases
- Time balance settings (for Accounts dimension)

### Calculation Scripts → DataJobs

Each calc script becomes a DataHub DataJob with:

- **Input/Output Datasets**: The cube being calculated
- **Column-Level Lineage**: Fine-grained mappings from calc operations
- **Source Code**: The full calc script content
- **Transform Operations**: Parsed calculation logic

### Fine-Grained Lineage

The connector extracts four types of lineage:

#### 1. Member Formula Lineage

From dynamic calc member formulas:

```
Revenue = "Quantity" * "Price"

→ Lineage: Measures.Quantity → Measures.Revenue
→ Lineage: Measures.Price → Measures.Revenue
```

#### 2. Consolidation Lineage

From parent-child hierarchy rollups:

```
Q1 = +Jan +Feb +Mar

→ Lineage: Year.Jan → Year.Q1 (SUM)
→ Lineage: Year.Feb → Year.Q1 (SUM)
→ Lineage: Year.Mar → Year.Q1 (SUM)
```

#### 3. Calc Script Lineage

From calculation scripts with FIX context:

```esscalc
FIX("Actual", "Working")
    "Variance" = "Actual" - "Budget";
ENDFIX
```

Produces lineage:

```yaml
- source_field: Scenario.Actual
  target_field: Scenario.Variance
  transformation_type: calc_script_formula
  filter_expression: 'Scenario IN ("Actual", "Working")'
```

#### 4. Load Rule Lineage

From data load rules:

```yaml
- source_dataset: sql://finance_db
  source_field: account_code
  target_field: Account
  transformation_type: load_rule_data
```

## YAML Intermediate Format

The extraction produces OpenLineage-compatible YAML:

```yaml
extraction_timestamp: '2024-01-15T10:30:00Z'
essbase_server: essbase.company.com
essbase_version: '21.4.0.0.0'

datasets:
  - namespace: essbase.company.com
    name: FinanceApp.Plan
    platform: essbase
    schema_fields:
      - name: Year
        type: dimension
        description: Time dimension with 12 members
        fields:
          - name: Q1
            type: member
            custom_properties:
              consolidation: "+"
              storage: store
    custom_properties:
      essbase_application: FinanceApp
      essbase_cube: Plan
      dimension_count: "5"
    tags:
      - essbase
      - multidimensional

jobs:
  - namespace: essbase.company.com
    name: FinanceApp.Plan.CalcAll
    job_type: essbase_calculation
    inputs:
      - namespace: essbase.company.com
        name: FinanceApp.Plan
    outputs:
      - namespace: essbase.company.com
        name: FinanceApp.Plan
    column_lineage:
      - source_field: Measures.Quantity
        target_field: Measures.Revenue
        transformation_type: formula
        transformation_expression: '"Quantity" * "Price"'
    source_code: |
      FIX("Actual")
        CALC DIM("Measures");
      ENDFIX

lineage_edges:
  - source_dataset: FinanceApp.Plan
    source_field: Year.Jan
    target_dataset: FinanceApp.Plan
    target_field: Year.Q1
    transformation_type: consolidation
    aggregation_function: SUM
```

## DataHub Entity Mapping

| Essbase Concept | DataHub Entity | Notes |
|-----------------|----------------|-------|
| Cube | Dataset | Platform: `essbase` |
| Dimension | SchemaField | With nested member fields |
| Member | SchemaField (nested) | Includes formula, consolidation |
| Calc Script | DataJob | Under DataFlow for app |
| Load Rule | DataJob | External source → cube |
| Member Formula | Fine-grained Lineage | Within dataset |
| Consolidation | Fine-grained Lineage | Parent-child rollup |

## Configuration Options

```python
EssbaseDataHubConnector(
    # Required
    essbase_url="https://essbase.company.com",
    essbase_user="admin",
    essbase_password="secret",
    datahub_server="http://datahub-gms:8080",
    
    # Optional
    datahub_token="your-token",           # DataHub auth token
    platform_instance="prod",              # DataHub platform instance
    env="PROD",                            # Environment (PROD, DEV, etc.)
    
    # Extraction options
    include_members=True,                  # Extract member details
    include_formulas=True,                 # Extract member formulas
    include_calc_scripts=True,             # Extract calc scripts
    include_load_rules=True,               # Extract load rules
    include_consolidation_lineage=True,    # Extract hierarchy lineage
    
    # Emission options
    dry_run=False,                         # Extract only, don't emit
)
```

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run linting
ruff check src/
black --check src/

# Type checking
mypy src/
```

## Troubleshooting

### Connection Issues

```bash
# Test Essbase connection
essbase-datahub list --essbase-url ... --essbase-user ... --essbase-password ...

# Test both connections
essbase-datahub test --essbase-url ... --datahub-server ...
```

### Large Cubes

For cubes with many members, you can limit extraction:

```python
schema_extractor = SchemaExtractor(
    client,
    include_members=True,
    max_member_depth=3,  # Only top 3 levels
)
```

### Missing Lineage

If lineage is incomplete:

1. Check calc script parsing with `--verbose`
2. Export YAML and inspect the `transforms` section
3. Verify member formulas are being extracted

## License

MIT License - see LICENSE file for details.
