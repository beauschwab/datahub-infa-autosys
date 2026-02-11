"""
Tests for Oracle stored procedure extraction and parsing.
"""
from datahub_custom_sources.extractors.oracle_stored_proc import (
    ProcedureSubstep,
    StoredProcedure,
    _classify_dml_operation,
    _extract_tables_from_sql,
    _parse_dml_statement,
    _parse_execute_immediate,
    normalize_table_name,
    parse_procedure_substeps,
)


def test_classify_dml_operation():
    """Test DML operation classification."""
    assert _classify_dml_operation("INSERT INTO orders VALUES (1, 2)") == "SQL_INSERT"
    assert _classify_dml_operation("UPDATE customers SET name = 'x'") == "SQL_UPDATE"
    assert _classify_dml_operation("DELETE FROM temp") == "SQL_DELETE"
    assert _classify_dml_operation("MERGE INTO target USING source") == "SQL_MERGE"
    assert _classify_dml_operation("SELECT * INTO v_var FROM dual") == "SQL_SELECT_INTO"
    assert _classify_dml_operation("DECLARE v_num NUMBER;") is None


def test_extract_tables_from_sql():
    """Test table extraction from SQL."""
    sql = "INSERT INTO orders SELECT * FROM customers JOIN products ON p.id = c.prod_id"
    tables = _extract_tables_from_sql(sql)
    assert "ORDERS" in tables
    assert "CUSTOMERS" in tables
    assert "PRODUCTS" in tables


def test_parse_dml_statement():
    """Test parsing a DML statement."""
    sql = "INSERT INTO etl.orders SELECT * FROM prod.customers"
    substep = _parse_dml_statement(sql, order=1, line_num=10)
    
    assert substep is not None
    assert substep.operation == "SQL_INSERT"
    assert "ETL.ORDERS" in substep.tables_written
    assert "PROD.CUSTOMERS" in substep.tables_read
    assert substep.order == 1
    assert substep.line_num == 10
    assert substep.query_hash is not None


def test_parse_execute_immediate():
    """Test parsing EXECUTE IMMEDIATE."""
    sql = "EXECUTE IMMEDIATE 'DELETE FROM staging WHERE processed = 1'"
    substep = _parse_execute_immediate(sql, order=2, line_num=20)
    
    assert substep is not None
    assert substep.operation == "DYNAMIC_SQL"
    assert "STAGING" in substep.tables_read or "STAGING" in substep.tables_written
    assert substep.sql_preview is not None
    assert "DELETE" in substep.sql_preview


def test_normalize_table_name():
    """Test table name normalization."""
    # Single name - add schema and db
    assert normalize_table_name("ORDERS", "ETL", "DW") == "DW.ETL.ORDERS"
    
    # Schema.table - add db
    assert normalize_table_name("ETL.ORDERS", "ETL", "DW") == "DW.ETL.ORDERS"
    
    # Fully qualified - keep as is
    assert normalize_table_name("DW.ETL.ORDERS", "ETL", "DW") == "DW.ETL.ORDERS"
    
    # No defaults
    assert normalize_table_name("ORDERS") == "ORDERS"


def test_parse_procedure_substeps():
    """Test parsing substeps from a procedure."""
    proc = StoredProcedure(
        owner="ETL",
        name="PROC_UPDATE_ORDERS",
        proc_type="PROCEDURE",
        source_lines=[
            "CREATE OR REPLACE PROCEDURE etl.proc_update_orders AS",
            "BEGIN",
            "  INSERT INTO etl.order_staging",
            "  SELECT * FROM prod.orders WHERE order_date = SYSDATE;",
            "  ",
            "  UPDATE etl.order_summary",
            "  SET total = (SELECT SUM(amount) FROM etl.order_staging);",
            "  ",
            "  EXECUTE IMMEDIATE 'DELETE FROM etl.order_staging WHERE processed = 1';",
            "END;",
        ],
    )
    
    parse_procedure_substeps(proc, parse_dynamic_sql=True)
    
    # Check substeps were extracted
    assert len(proc.substeps) >= 3  # INSERT, UPDATE, DYNAMIC_SQL
    
    # Check operations
    operations = [s.operation for s in proc.substeps]
    assert "SQL_INSERT" in operations
    assert "SQL_UPDATE" in operations
    assert "DYNAMIC_SQL" in operations
    
    # Check tables
    assert len(proc.all_tables_read) > 0
    assert len(proc.all_tables_written) > 0
    assert proc.has_dynamic_sql


def test_parse_procedure_with_nested_calls():
    """Test parsing procedure with nested calls."""
    proc = StoredProcedure(
        owner="ETL",
        name="PROC_MAIN",
        proc_type="PROCEDURE",
        source_lines=[
            "CREATE OR REPLACE PROCEDURE etl.proc_main AS",
            "BEGIN",
            "  INSERT INTO etl.staging SELECT * FROM prod.source;",
            "  etl.proc_calculate_metrics();",
            "  etl.proc_cleanup();",
            "END;",
        ],
    )
    
    parse_procedure_substeps(proc, parse_dynamic_sql=True)
    
    # Check nested calls detected
    assert len(proc.nested_calls) >= 2
    assert any("PROC_CALCULATE_METRICS" in call for call in proc.nested_calls)
    assert any("PROC_CLEANUP" in call for call in proc.nested_calls)
    
    # Check procedure call substeps
    proc_call_substeps = [s for s in proc.substeps if s.operation == "PROCEDURE_CALL"]
    assert len(proc_call_substeps) >= 2
