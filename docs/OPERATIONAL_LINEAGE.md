# Oracle operational lineage: what to pull and why

Operational lineage answers: "What actually ran, when, by whom, touching which tables/columns/partitions?"

## Useful fields from Oracle telemetry (best-effort)

### Statement-level (V$SQL / DBA_HIST_SQLTEXT / audit trails)
- SQL_ID, PLAN_HASH_VALUE: stable identifiers for a statement
- SQL_TEXT (or normalized SQL): for parsing datasets and (sometimes) column-level lineage
- PARSING_SCHEMA_NAME: which schema submitted the statement
- MODULE / ACTION / CLIENT_IDENTIFIER: critical for mapping activity back to your scheduler / app (set these in your app!)
- LAST_ACTIVE_TIME, FIRST_LOAD_TIME, ELAPSED_TIME, CPU_TIME, BUFFER_GETS, DISK_READS: performance + timing context
- EXECUTIONS, ROWS_PROCESSED: impact & “what changed” indicators

### Session-level (V$SESSION / unified audit)
- USERNAME, OSUSER, MACHINE, PROGRAM: “who/what”
- LOGON_TIME, STATUS: timing / liveness
- SID/SERIAL#: join keys to statement or audit records

### Object-level access (Unified audit / FGA)
- Object owner/name + access type (SELECT/INSERT/UPDATE/DELETE/EXECUTE)
- Timestamp, user, terminal
- For FGA: policy name, sql_text, bind variables (optional)

### PL/SQL stored procedure execution
- Procedure/function name, schema, arguments (if available)
- Any dynamic SQL the proc executes (hardest part)
- Objects touched inside the proc (requires tracing/auditing or static code analysis)

## Mapping to DataHub entities

- Use **DataProcessInstance** for *each run* (AutoSys job run, Informatica workflow run, Oracle stored-proc invocation, etc.).
- Put the *template* as the DataJob (AutoSys job template / Informatica workflow template).
- For nested orchestration, use DataProcessInstance **relationships**:
  - Box-run instance is parent; job-run instances are children.
- Put actual tables read/written in DataProcessInstance **inputs/outputs**.
- Attach partition keys/predicates and run ids as **edge properties** or **custom properties** on the instance.

This repo ships a reference runner (`dh_infa_autosys.operational.oracle_runner`) that polls V$SQL and emits short-lived instances.
In production, most teams graduate to Unified Audit + MODULE/ACTION tagging + join to scheduler run ids.
