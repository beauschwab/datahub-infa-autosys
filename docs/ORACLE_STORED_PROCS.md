# Oracle Stored Procedures: Dynamic SQL Parsing

This feature extends the Oracle operational lineage module to extract and parse stored procedures with fine-grained lineage tracking.

## Overview

Many Oracle stored procedures contain dynamic SQL that cannot be parsed by standard static analysis tools. This module extracts stored procedures from Oracle's data dictionary views (`ALL_SOURCE`, `DBA_SOURCE`, `USER_SOURCE`) and parses the PL/SQL code to:

1. Extract substeps (DML statements, dynamic SQL, nested calls)
2. Identify table dependencies (reads/writes)
3. Create DataFlow/DataJob entities with fine-grained lineage
4. Emit transformOperation facets for each substep

## Architecture

### DataHub Entity Model

```
DataFlow: "oracle_stored_procedures"
  └─ DataJob: "schema.proc_name" (one per stored procedure)
       ├─ DataJobInfo
       │   ├─ type: "ORACLE_PROCEDURE" / "ORACLE_FUNCTION" / "ORACLE_PACKAGE_BODY"
       │   └─ customProperties:
       │        ├─ substeps: JSON array of substep metadata
       │        ├─ nested_procedures: comma-separated list
       │        └─ has_dynamic_sql: true/false
       ├─ DataJobInputOutput
       │   ├─ inputDatasets: all tables read
       │   ├─ outputDatasets: all tables written
       │   └─ inputDatajobEdges: nested procedure calls
       └─ UpstreamLineage (per output dataset)
            └─ FineGrainedLineages: one per substep
                 ├─ transformOperation: "SQL_INSERT", "SQL_UPDATE", etc.
                 ├─ upstreams: field URNs for tables read
                 ├─ downstreams: field URNs for tables written
                 └─ query: SQL preview text
```

## Configuration

Add these fields to your `OracleOperationalConfig`:

```yaml
# Enable stored procedure extraction
extract_stored_procedures: true

# Filter by schema names (empty = all accessible)
stored_proc_schemas:
  - ETL
  - REPORTING
  - DATA_WAREHOUSE

# Filter by name patterns (SQL LIKE syntax)
stored_proc_patterns:
  - "ETL_%"
  - "PROC_%"
  - "SP_%"

# Parse EXECUTE IMMEDIATE statements
parse_dynamic_sql: true

# Max depth for nested procedure resolution
max_proc_nesting_depth: 5

# Source view: all_source, dba_source, or user_source
stored_proc_source: "all_source"
```

## Usage

### CLI Command

Extract and emit stored procedures to DataHub:

```bash
dhcs extract-oracle-procs \
  --config examples/configs/oracle_stored_procs.json \
  --server http://localhost:8080
```

### Programmatic API

```python
from datahub_custom_sources.config import OracleOperationalConfig
from datahub_custom_sources.operational import emit_stored_procedures

cfg = OracleOperationalConfig.model_validate({
    "dsn": "host/service",
    "user": "etl_user",
    "password_env": "ORACLE_PASSWORD",
    "extract_stored_procedures": True,
    "stored_proc_schemas": ["ETL"],
    # ... other config
})

count = emit_stored_procedures(cfg, server="http://localhost:8080")
print(f"Emitted {count} procedures")
```

## Supported Operations

The parser identifies these operation types (`transformOperation` values):

| Operation | Description |
|-----------|-------------|
| `SQL_INSERT` | INSERT statement within procedure |
| `SQL_UPDATE` | UPDATE statement within procedure |
| `SQL_DELETE` | DELETE statement within procedure |
| `SQL_MERGE` | MERGE statement within procedure |
| `SQL_SELECT_INTO` | SELECT...INTO variable assignment |
| `DYNAMIC_SQL` | EXECUTE IMMEDIATE / dynamic SQL |
| `PROCEDURE_CALL` | Nested procedure invocation |

## Example: Parsing Results

Given this stored procedure:

```sql
CREATE OR REPLACE PROCEDURE ETL.PROC_UPDATE_ORDERS AS
BEGIN
  -- Substep 1: SQL_INSERT
  INSERT INTO ETL.ORDER_STAGING
  SELECT * FROM PROD.ORDERS WHERE order_date = SYSDATE;
  
  -- Substep 2: SQL_UPDATE
  UPDATE ETL.ORDER_SUMMARY
  SET total_amount = (SELECT SUM(amount) FROM ETL.ORDER_STAGING);
  
  -- Substep 3: PROCEDURE_CALL
  ETL.PROC_CALCULATE_METRICS();
  
  -- Substep 4: DYNAMIC_SQL
  EXECUTE IMMEDIATE 'DELETE FROM ETL.ORDER_STAGING WHERE processed = 1';
END;
```

The parser emits:

**DataJob**: `urn:li:dataJob:(oracle,oracle_stored_procedures,PROD,etl.proc_update_orders)`
- **Inputs**: `PROD.ORDERS`, `ETL.ORDER_STAGING`
- **Outputs**: `ETL.ORDER_STAGING`, `ETL.ORDER_SUMMARY`
- **Substeps** (4 transformOperation entries):
  1. `SQL_INSERT`: `PROD.ORDERS` → `ETL.ORDER_STAGING`
  2. `SQL_UPDATE`: `ETL.ORDER_STAGING` → `ETL.ORDER_SUMMARY`
  3. `PROCEDURE_CALL`: → `etl.proc_calculate_metrics`
  4. `DYNAMIC_SQL`: `ETL.ORDER_STAGING` → `ETL.ORDER_STAGING`

## Limitations

### Known Limitations

1. **Static Parsing Only**: The parser analyzes source code, not runtime behavior
   - Dynamic table names (variables in SQL) cannot be resolved
   - Conditional logic (IF/CASE) is not evaluated
   
2. **Simplified PL/SQL Parsing**:
   - Not a full AST parser (uses regex + heuristics)
   - Multi-line statements may be missed if not properly terminated
   - Complex PL/SQL constructs (cursors, records, etc.) are simplified

3. **V$SQL Integration**:
   - Currently separate from operational lineage (v$sql polling)
   - Future: correlate stored proc calls in v$sql with static definitions

### Workarounds

For complex procedures:
- Use `stored_proc_patterns` to focus on well-structured ETL procedures
- Set `parse_dynamic_sql: false` if dynamic SQL causes parsing errors
- Manually verify critical procedures via DataHub UI

## Integration with V$SQL

### Current State

Two separate extraction paths:
1. **Stored Procedures** (this module): Static analysis of PL/SQL source
2. **V$SQL** (`oracle_runner.py`): Live SQL execution tracking

### Future Enhancement

Planned correlation:
- Detect stored procedure calls in v$sql via `MODULE`/`ACTION` tags
- Link `DataProcessInstance` (runtime) to `DataJob` (template)
- Merge static + runtime lineage for complete picture

Example future workflow:
```python
# 1) Extract static definitions
emit_stored_procedures(cfg, server)

# 2) Poll v$sql for executions
emit_operational_lineage(cfg, datajob_urn, server)

# 3) DataHub correlates via URN matching
```

## Performance Considerations

- **Extraction Time**: ~1-5 seconds per 100 procedures (depends on source code size)
- **API Calls**: One MCP per procedure + lineage aspects (batching recommended for 1000+ procedures)
- **Database Load**: Minimal (reads from data dictionary views, no execution)

## Permissions Required

The Oracle user needs:
```sql
-- For all_source (recommended)
GRANT SELECT ON ALL_SOURCE TO your_user;

-- For dba_source (requires DBA role)
GRANT SELECT ON DBA_SOURCE TO your_user;

-- For user_source (own schema only)
-- No grant needed - automatically accessible
```

## See Also

- [Oracle Operational Lineage](OPERATIONAL_LINEAGE.md) - V$SQL polling for runtime lineage
- [DataHub DataJob Model](https://datahubproject.io/docs/generated/metamodel/entities/datajob)
- [Fine-Grained Lineage](https://datahubproject.io/docs/lineage/lineage-feature-guide/#column-level-lineage)
