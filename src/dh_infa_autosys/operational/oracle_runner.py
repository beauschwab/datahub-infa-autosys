from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

import oracledb
from rich.console import Console

from datahub.api.entities.dataprocess.dataprocess_instance import DataProcessInstance
from datahub.emitter.rest_emitter import DatahubRestEmitter

from dh_infa_autosys.config import OracleOperationalConfig
from dh_infa_autosys.utils.urns import dataset_urn

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
