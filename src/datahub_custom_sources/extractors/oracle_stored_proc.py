from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

import oracledb

logger = logging.getLogger(__name__)


# Regex patterns for parsing PL/SQL
_DML_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE)\s+(?:INTO\s+)?(\w+(?:\.\w+)*)",
    re.I | re.DOTALL,
)
_SELECT_INTO_PATTERN = re.compile(
    r"\bSELECT\s+.+?\s+INTO\s+",
    re.I | re.DOTALL,
)
_TABLE_REF_PATTERN = re.compile(
    r"\b(FROM|JOIN|INTO|UPDATE|MERGE\s+INTO)\s+((?:(?:\w+\.){0,2})\w+)\b",
    re.I,
)
_EXECUTE_IMMEDIATE_PATTERN = re.compile(
    r"\bEXECUTE\s+IMMEDIATE\s+([^;]+)",
    re.I | re.DOTALL,
)
_PROCEDURE_CALL_PATTERN = re.compile(
    r"\b(?:CALL\s+|EXECUTE\s+)?(\w+(?:\.\w+)*)\s*\(",
    re.I,
)


@dataclass
class ProcedureSubstep:
    """Represents a single operation within a stored procedure."""
    order: int
    operation: str  # SQL_INSERT, SQL_UPDATE, PROCEDURE_CALL, etc.
    tables_read: Set[str] = field(default_factory=set)
    tables_written: Set[str] = field(default_factory=set)
    sql_preview: Optional[str] = None
    query_hash: Optional[str] = None
    nested_proc_name: Optional[str] = None
    line_num: int = 0


@dataclass
class StoredProcedure:
    """Metadata for a single stored procedure."""
    owner: str
    name: str
    proc_type: str  # PROCEDURE, FUNCTION, PACKAGE BODY
    source_lines: List[str]
    substeps: List[ProcedureSubstep] = field(default_factory=list)
    nested_calls: Set[str] = field(default_factory=set)
    has_dynamic_sql: bool = False
    all_tables_read: Set[str] = field(default_factory=set)
    all_tables_written: Set[str] = field(default_factory=set)

    @property
    def line_count(self) -> int:
        return len(self.source_lines)

    @property
    def full_source(self) -> str:
        return "\n".join(self.source_lines)


def extract_procedures(
    conn: oracledb.Connection,
    source_view: str = "all_source",
    schemas: Optional[List[str]] = None,
    name_patterns: Optional[List[str]] = None,
) -> List[StoredProcedure]:
    """
    Extract stored procedure source code from Oracle dictionary views.
    
    Args:
        conn: Oracle database connection
        source_view: 'all_source', 'dba_source', or 'user_source'
        schemas: Filter to specific schemas (None = all accessible)
        name_patterns: SQL LIKE patterns for procedure names (None = all)
    
    Returns:
        List of StoredProcedure objects with source code
    """
    sql = f"""
    SELECT owner, name, type, line, text
    FROM {source_view}
    WHERE type IN ('PROCEDURE', 'FUNCTION', 'PACKAGE BODY')
    """
    
    conditions = []
    if schemas:
        schema_list = ", ".join(f"'{s.upper()}'" for s in schemas)
        conditions.append(f"owner IN ({schema_list})")
    
    if name_patterns:
        pattern_conditions = " OR ".join(f"name LIKE '{p}'" for p in name_patterns)
        conditions.append(f"({pattern_conditions})")
    
    if conditions:
        sql += " AND " + " AND ".join(conditions)
    
    sql += " ORDER BY owner, name, type, line"
    
    logger.info(f"Querying {source_view} for stored procedures...")
    cur = conn.cursor()
    cur.execute(sql)
    
    # Group lines by (owner, name, type)
    proc_sources: Dict[Tuple[str, str, str], List[str]] = {}
    for row in cur.fetchall():
        owner, name, proc_type, line, text = row
        key = (str(owner), str(name), str(proc_type))
        if key not in proc_sources:
            proc_sources[key] = []
        proc_sources[key].append(str(text).rstrip())
    
    procedures = []
    for (owner, name, proc_type), lines in proc_sources.items():
        proc = StoredProcedure(
            owner=owner,
            name=name,
            proc_type=proc_type,
            source_lines=lines,
        )
        procedures.append(proc)
    
    logger.info(f"Extracted {len(procedures)} stored procedures")
    return procedures


def _classify_dml_operation(sql_snippet: str) -> Optional[str]:
    """Classify DML operation type from SQL snippet."""
    sql_upper = sql_snippet.upper().strip()
    if sql_upper.startswith("INSERT"):
        return "SQL_INSERT"
    elif sql_upper.startswith("UPDATE"):
        return "SQL_UPDATE"
    elif sql_upper.startswith("DELETE"):
        return "SQL_DELETE"
    elif sql_upper.startswith("MERGE"):
        return "SQL_MERGE"
    elif "SELECT" in sql_upper and "INTO" in sql_upper:
        return "SQL_SELECT_INTO"
    return None


def _extract_tables_from_sql(sql: str) -> Set[str]:
    """Extract table references from SQL text using regex."""
    tables = set()
    for _, table in _TABLE_REF_PATTERN.findall(sql):
        tables.add(table.strip().upper())
    return tables


def _parse_dml_statement(
    sql_snippet: str,
    order: int,
    line_num: int,
) -> Optional[ProcedureSubstep]:
    """Parse a DML statement and extract table references."""
    operation = _classify_dml_operation(sql_snippet)
    if not operation:
        return None
    
    tables = _extract_tables_from_sql(sql_snippet)
    
    # Classify as read/write based on operation
    tables_read = set()
    tables_written = set()
    
    if operation in ("SQL_INSERT", "SQL_UPDATE", "SQL_DELETE", "SQL_MERGE"):
        # First table is typically the target (written)
        match = _DML_PATTERN.search(sql_snippet)
        if match:
            target_table = match.group(2).upper()
            tables_written.add(target_table)
            # Other tables in FROM/JOIN clauses are read
            tables_read = tables - {target_table}
        else:
            # Fallback: assume all tables are involved
            tables_written = tables
    elif operation == "SQL_SELECT_INTO":
        # SELECT INTO reads from tables
        tables_read = tables
    
    sql_hash = hashlib.sha256(sql_snippet.encode()).hexdigest()[:16]
    
    return ProcedureSubstep(
        order=order,
        operation=operation,
        tables_read=tables_read,
        tables_written=tables_written,
        sql_preview=sql_snippet[:500] if len(sql_snippet) > 500 else sql_snippet,
        query_hash=sql_hash,
        line_num=line_num,
    )


def _parse_execute_immediate(
    sql_snippet: str,
    order: int,
    line_num: int,
) -> Optional[ProcedureSubstep]:
    """Parse EXECUTE IMMEDIATE statement and extract dynamic SQL."""
    match = _EXECUTE_IMMEDIATE_PATTERN.search(sql_snippet)
    if not match:
        return None
    
    dynamic_sql = match.group(1).strip()
    # Remove string quotes if present
    dynamic_sql = dynamic_sql.strip("'\"")
    
    # Try to extract tables from dynamic SQL
    tables = _extract_tables_from_sql(dynamic_sql)
    
    sql_hash = hashlib.sha256(dynamic_sql.encode()).hexdigest()[:16]
    
    return ProcedureSubstep(
        order=order,
        operation="DYNAMIC_SQL",
        tables_read=tables,
        tables_written=tables,  # Conservative: assume both read/write
        sql_preview=dynamic_sql[:500],
        query_hash=sql_hash,
        line_num=line_num,
    )


def _find_procedure_calls(source_code: str) -> Set[str]:
    """Find nested procedure calls in PL/SQL source code."""
    proc_calls = set()
    for match in _PROCEDURE_CALL_PATTERN.finditer(source_code):
        proc_name = match.group(1).upper()
        # Filter out common PL/SQL built-ins
        if proc_name not in {
            "DBMS_OUTPUT.PUT_LINE",
            "DBMS_SQL",
            "UTL_FILE",
            "RAISE_APPLICATION_ERROR",
            "TO_CHAR",
            "TO_DATE",
            "NVL",
            "DECODE",
        }:
            proc_calls.add(proc_name)
    return proc_calls


def parse_procedure_substeps(
    proc: StoredProcedure,
    parse_dynamic_sql: bool = True,
) -> None:
    """
    Parse stored procedure source code and extract substeps.
    
    Modifies proc in place by populating substeps, nested_calls, and table references.
    """
    source = proc.full_source
    
    # Find nested procedure calls
    proc.nested_calls = _find_procedure_calls(source)
    
    # Split source into statements (simplified - real parser would need PL/SQL AST)
    # We'll use line-by-line scanning with some context awareness
    substeps = []
    order = 0
    
    # Scan for DML statements
    for i, line in enumerate(proc.source_lines, start=1):
        line_upper = line.upper().strip()
        
        # Skip comments and empty lines
        if not line_upper or line_upper.startswith("--") or line_upper.startswith("/*"):
            continue
        
        # Look for DML keywords at start of statement
        if any(line_upper.startswith(kw) for kw in ["INSERT", "UPDATE", "DELETE", "MERGE"]):
            # Collect multi-line statement (simple heuristic)
            stmt_lines = [line]
            j = i
            while j < len(proc.source_lines):
                if ";" in proc.source_lines[j]:
                    break
                j += 1
                if j < len(proc.source_lines):
                    stmt_lines.append(proc.source_lines[j])
            
            sql_snippet = " ".join(stmt_lines)
            substep = _parse_dml_statement(sql_snippet, order, i)
            if substep:
                substeps.append(substep)
                proc.all_tables_read.update(substep.tables_read)
                proc.all_tables_written.update(substep.tables_written)
                order += 1
        
        # Look for SELECT INTO
        elif "SELECT" in line_upper and "INTO" in line_upper:
            stmt_lines = [line]
            j = i
            while j < len(proc.source_lines):
                if ";" in proc.source_lines[j]:
                    break
                j += 1
                if j < len(proc.source_lines):
                    stmt_lines.append(proc.source_lines[j])
            
            sql_snippet = " ".join(stmt_lines)
            substep = _parse_dml_statement(sql_snippet, order, i)
            if substep:
                substeps.append(substep)
                proc.all_tables_read.update(substep.tables_read)
                order += 1
        
        # Look for EXECUTE IMMEDIATE (dynamic SQL)
        if parse_dynamic_sql and "EXECUTE IMMEDIATE" in line_upper:
            proc.has_dynamic_sql = True
            stmt_lines = [line]
            j = i
            while j < len(proc.source_lines):
                if ";" in proc.source_lines[j]:
                    break
                j += 1
                if j < len(proc.source_lines):
                    stmt_lines.append(proc.source_lines[j])
            
            sql_snippet = " ".join(stmt_lines)
            substep = _parse_execute_immediate(sql_snippet, order, i)
            if substep:
                substeps.append(substep)
                proc.all_tables_read.update(substep.tables_read)
                proc.all_tables_written.update(substep.tables_written)
                order += 1
    
    # Add substeps for nested procedure calls
    for nested_proc in proc.nested_calls:
        order += 1
        substeps.append(
            ProcedureSubstep(
                order=order,
                operation="PROCEDURE_CALL",
                nested_proc_name=nested_proc,
            )
        )
    
    proc.substeps = substeps
    logger.debug(
        f"Parsed {proc.owner}.{proc.name}: {len(substeps)} substeps, "
        f"{len(proc.nested_calls)} nested calls, dynamic_sql={proc.has_dynamic_sql}"
    )


def normalize_table_name(
    table: str,
    default_schema: Optional[str] = None,
    default_db: Optional[str] = None,
) -> str:
    """
    Normalize table reference to fully qualified name.
    
    Oracle convention: [db.]schema.table
    """
    parts = table.split(".")
    if len(parts) == 1:
        # Just table name - add schema and db
        name = parts[0]
        fqn = ".".join([p for p in [default_db, default_schema, name] if p])
        return fqn
    elif len(parts) == 2:
        # schema.table - add db if provided
        schema, name = parts
        fqn = ".".join([p for p in [default_db, schema, name] if p])
        return fqn
    else:
        # Already qualified or 3+ parts - keep as is
        return table
