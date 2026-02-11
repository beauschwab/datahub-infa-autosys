from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

import oracledb
from rich.console import Console

from datahub.api.entities.dataprocess.dataprocess_instance import DataProcessInstance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

from datahub_custom_sources.config import OracleOperationalConfig
from datahub_custom_sources.emit.builders import (
    make_edge,
    mcp_dataflow_info,
    mcp_datajob_info,
    mcp_datajob_io,
    mcp_upstream_lineage,
)
from datahub_custom_sources.extractors.oracle_stored_proc import (
    StoredProcedure,
    extract_procedures,
    normalize_table_name,
    parse_procedure_substeps,
)
from datahub_custom_sources.utils.urns import dataflow_urn, datajob_urn, dataset_urn

console = Console()

_TABLE_RE = re.compile(
    r"\b(from|join|into|update|merge\s+into)\s+((?:(?:\w+\.){0,2})\w+)\b",
    re.I,
)


@dataclass(frozen=True)
class ObservedStatement:
    sql_id: str
    parsing_schema: Optional[str]
    sql_text: str
    last_active_time: datetime
    executions: int


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _extract_tables(sql: str) -> Set[str]:
    tables = set()
    for _, t in _TABLE_RE.findall(sql or ""):
        tables.add(t.strip())
    return tables


def _normalize_table(table: str, default_db: Optional[str], default_schema: Optional[str]) -> str:
    # Oracle typically uses schema.table; db is optional / abstract in many orgs.
    parts = table.split(".")
    if len(parts) == 1:
        sch = default_schema
        name = parts[0]
        fqn = ".".join([p for p in [default_db, sch, name] if p])
        return fqn
    if len(parts) == 2:
        sch, name = parts
        fqn = ".".join([p for p in [default_db, sch, name] if p])
        return fqn
    if len(parts) >= 3:
        # db.schema.table - keep last 3
        return ".".join(parts[-3:])
    return table


def _connect(cfg: OracleOperationalConfig) -> oracledb.Connection:
    pw = os.environ.get(cfg.password_env)
    if not pw:
        raise RuntimeError(f"Missing Oracle password env var: {cfg.password_env}")
    # Thin mode default. For thick mode, configure ORACLE_HOME etc.
    return oracledb.connect(user=cfg.user, password=pw, dsn=cfg.dsn)


def poll_vsql(
    conn: oracledb.Connection,
    since_utc: datetime,
    max_rows: int = 2000,
) -> List[ObservedStatement]:
    """
    Pull statements from V$SQL which can support operational lineage.
    Requires permissions (often via DBA views or grants).

    NOTE: V$SQL has rolling history and can miss very short-lived statements.
    For stronger auditing, consider Unified Audit Trail / Fine-Grained Auditing.
    """
    sql = """
    SELECT
      sql_id,
      parsing_schema_name,
      sql_text,
      last_active_time,
      executions
    FROM v$sql
    WHERE last_active_time >= :since_time
    ORDER BY last_active_time ASC
    FETCH FIRST :max_rows ROWS ONLY
    """
    cur = conn.cursor()
    cur.execute(sql, since_time=since_utc.replace(tzinfo=None), max_rows=max_rows)
    out: List[ObservedStatement] = []
    for row in cur.fetchall():
        sql_id, schema_name, sql_text, last_active, execs = row
        # Oracle returns naive datetime; treat as UTC-ish (configure in your environment).
        last_active_dt = last_active.replace(tzinfo=timezone.utc) if isinstance(last_active, datetime) else _now_utc()
        out.append(
            ObservedStatement(
                sql_id=str(sql_id),
                parsing_schema=str(schema_name) if schema_name else None,
                sql_text=str(sql_text) if sql_text else "",
                last_active_time=last_active_dt,
                executions=int(execs) if execs is not None else 0,
            )
        )
    return out


def emit_operational_lineage(
    cfg: OracleOperationalConfig,
    datajob_urn: str,
    server: str,
    stop_after_loops: Optional[int] = None,
) -> None:
    """
    Reference streaming runner:
      - polls Oracle statement activity
      - extracts datasets touched
      - emits DataProcessInstance start/end + inputs/outputs (best-effort)

    This is intentionally a starting point. In production you will likely:
      - join to application run ids / scheduler logs / module/action tags
      - distinguish reads vs writes (DML parsing)
      - capture bind variables & partition predicates
      - map SQL_ID to parent AutoSys/Informatica run ids
    """
    emitter = DatahubRestEmitter(gms_server=server)
    conn = _connect(cfg)

    loops = 0
    cursor_time = _now_utc() - timedelta(minutes=cfg.lookback_minutes)
    console.log(f"Starting Oracle operational lineage polling from {cursor_time.isoformat()}")

    while True:
        loops += 1
        stmts: List[ObservedStatement] = []
        if "v$sql" in [s.lower() for s in cfg.query_sources]:
            stmts = poll_vsql(conn, since_utc=cursor_time)

        for st in stmts:
            cursor_time = max(cursor_time, st.last_active_time + timedelta(seconds=1))

            tables = {_normalize_table(t, cfg.lineage.default_db, cfg.lineage.default_schema) for t in _extract_tables(st.sql_text)}
            if not tables:
                continue

            # Heuristic: classify outputs if SQL contains INSERT/UPDATE/MERGE/DELETE
            sql_lower = st.sql_text.lower()
            is_write = any(k in sql_lower for k in [" insert ", " update ", " merge ", " delete "])

            inputs = [dataset_urn(cfg.lineage.platform, t, cfg.lineage.env, cfg.lineage.platform_instance) for t in tables]
            outputs = inputs if is_write else []

            dpi = DataProcessInstance.from_datajob(datajob_urn, data_process_instance_id=f"oracle_sql:{st.sql_id}")
            # We emit a very short-lived instance with timestamps derived from last_active_time.
            started = int(st.last_active_time.timestamp() * 1000)
            dpi.emit_process_start(emitter, start_timestamp_millis=started)
            if inputs:
                dpi.emit_process_inputs(emitter, input_datasets=inputs)
            if outputs:
                dpi.emit_process_outputs(emitter, output_datasets=outputs)
            dpi.emit_process_end(
                emitter,
                end_timestamp_millis=started,
                result_status="SUCCESS",
                result_type="COMPLETE",
            )

        if stop_after_loops and loops >= stop_after_loops:
            break
        time.sleep(cfg.poll_interval_s)


def emit_stored_procedures(
    cfg: OracleOperationalConfig,
    server: str,
) -> int:
    """
    Extract and emit Oracle stored procedures as DataFlow + DataJobs with fine-grained lineage.
    
    Returns the number of procedures emitted.
    """
    conn = _connect(cfg)
    emitter = DatahubRestEmitter(gms_server=server)
    
    console.log("Extracting stored procedures from Oracle...")
    procedures = extract_procedures(
        conn,
        source_view=cfg.stored_proc_source,
        schemas=cfg.stored_proc_schemas if cfg.stored_proc_schemas else None,
        name_patterns=cfg.stored_proc_patterns if cfg.stored_proc_patterns else None,
    )
    
    if not procedures:
        console.log("No stored procedures found")
        return 0
    
    console.log(f"Found {len(procedures)} stored procedures, parsing substeps...")
    
    # Parse all procedures
    for proc in procedures:
        parse_procedure_substeps(proc, parse_dynamic_sql=cfg.parse_dynamic_sql)
    
    # Create parent DataFlow for all stored procedures
    flow_id = "oracle_stored_procedures"
    flow_urn = dataflow_urn(cfg.lineage.platform, flow_id, cfg.lineage.env)
    
    mcps: List[MetadataChangeProposalWrapper] = []
    mcps.append(
        mcp_dataflow_info(
            flow_urn,
            name="Oracle Stored Procedures",
            description=f"Stored procedures from Oracle schemas: {', '.join(cfg.stored_proc_schemas) if cfg.stored_proc_schemas else 'all'}",
        )
    )
    
    # Emit each procedure as a DataJob
    for proc in procedures:
        job_id = f"{proc.owner}.{proc.name}".lower()
        job_urn = datajob_urn(flow_urn, job_id)
        
        # Prepare custom properties
        custom_props = {
            "owner": proc.owner,
            "proc_type": proc.proc_type,
            "source_lines": str(proc.line_count),
            "substep_count": str(len(proc.substeps)),
            "has_dynamic_sql": str(proc.has_dynamic_sql),
        }
        
        if proc.nested_calls:
            custom_props["nested_procedures"] = ", ".join(sorted(proc.nested_calls))
        
        # Add substeps metadata as JSON
        if proc.substeps:
            substeps_metadata = [
                {
                    "order": s.order,
                    "operation": s.operation,
                    "line": s.line_num,
                    "tables_read": list(s.tables_read),
                    "tables_written": list(s.tables_written),
                    "nested_proc": s.nested_proc_name,
                }
                for s in proc.substeps
            ]
            custom_props["substeps"] = json.dumps(substeps_metadata)
        
        # Emit DataJob info
        mcps.append(
            mcp_datajob_info(
                job_urn=job_urn,
                name=f"{proc.owner}.{proc.name}",
                job_type=f"ORACLE_{proc.proc_type.upper()}",
                flow_urn=flow_urn,
                description=f"{proc.proc_type} with {len(proc.substeps)} substeps",
                custom_properties=custom_props,
            )
        )
        
        # Normalize all table names
        all_inputs = {
            normalize_table_name(t, cfg.lineage.default_schema, cfg.lineage.default_db)
            for t in proc.all_tables_read
        }
        all_outputs = {
            normalize_table_name(t, cfg.lineage.default_schema, cfg.lineage.default_db)
            for t in proc.all_tables_written
        }
        
        # Create dataset URNs
        input_dataset_urns = [
            dataset_urn(cfg.lineage.platform, t, cfg.lineage.env, cfg.lineage.platform_instance)
            for t in sorted(all_inputs)
        ]
        output_dataset_urns = [
            dataset_urn(cfg.lineage.platform, t, cfg.lineage.env, cfg.lineage.platform_instance)
            for t in sorted(all_outputs)
        ]
        
        # Emit DataJobInputOutput (coarse-grained dataset lineage)
        if input_dataset_urns or output_dataset_urns:
            input_edges = [make_edge(d) for d in input_dataset_urns]
            output_edges = [make_edge(d) for d in output_dataset_urns]
            
            # Handle job->job edges for nested procedure calls
            input_job_edges = []
            for nested_proc in proc.nested_calls:
                # Try to find the nested procedure URN
                nested_job_id = f"{proc.owner}.{nested_proc}".lower()
                nested_job_urn = datajob_urn(flow_urn, nested_job_id)
                input_job_edges.append(
                    make_edge(nested_job_urn, properties={"relationship": "procedure_call"})
                )
            
            mcps.append(
                mcp_datajob_io(
                    job_urn=job_urn,
                    input_datasets=input_dataset_urns,
                    output_datasets=output_dataset_urns,
                    input_dataset_edges=input_edges if input_edges else None,
                    output_dataset_edges=output_edges if output_edges else None,
                    input_datajob_edges=input_job_edges if input_job_edges else None,
                )
            )
        
        # Emit fine-grained lineage (transformOperation per substep)
        if output_dataset_urns and proc.substeps:
            fine_grained_mappings = []
            
            for substep in proc.substeps:
                # Normalize substep tables
                substep_inputs = {
                    normalize_table_name(t, cfg.lineage.default_schema, cfg.lineage.default_db)
                    for t in substep.tables_read
                }
                substep_outputs = {
                    normalize_table_name(t, cfg.lineage.default_schema, cfg.lineage.default_db)
                    for t in substep.tables_written
                }
                
                if not substep_inputs and not substep_outputs:
                    continue
                
                # Create field URNs (using wildcard "*" for table-level lineage)
                upstream_field_paths = [f"{t}.*" for t in sorted(substep_inputs)]
                downstream_field_paths = [f"{t}.*" for t in sorted(substep_outputs)]
                
                # For dataset-level fine-grained lineage, we need to pick representative datasets
                # We'll emit one mapping per output table
                for output_table in substep_outputs:
                    output_ds_urn = dataset_urn(
                        cfg.lineage.platform,
                        output_table,
                        cfg.lineage.env,
                        cfg.lineage.platform_instance,
                    )
                    
                    # Map to this output from all inputs in this substep
                    if substep_inputs:
                        fine_grained_mappings.append(
                            (
                                substep.operation,  # transformOperation
                                upstream_field_paths,  # upstream fields
                                [f"{output_table}.*"],  # downstream field
                                substep.sql_preview,  # query text
                            )
                        )
            
            # Emit fine-grained lineage for each output dataset
            for output_ds_urn in output_dataset_urns:
                if fine_grained_mappings:
                    mcps.append(
                        mcp_upstream_lineage(
                            downstream_dataset_urn=output_ds_urn,
                            upstream_dataset_urns=input_dataset_urns,
                            fine_grained_field_mappings=fine_grained_mappings,
                        )
                    )
    
    # Emit all MCPs
    console.log(f"Emitting {len(mcps)} metadata change proposals to DataHub...")
    for mcp in mcps:
        emitter.emit_mcp(mcp)
    
    conn.close()
    console.log(f"Successfully emitted {len(procedures)} stored procedures")
    return len(procedures)
