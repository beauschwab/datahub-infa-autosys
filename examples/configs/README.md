# Oracle Stored Procedures Configuration Example

This configuration file demonstrates how to extract Oracle stored procedures with fine-grained lineage.

## Usage

```bash
dhcs extract-oracle-procs \
  --config examples/configs/oracle_stored_procs.json \
  --server http://localhost:8080
```

## Configuration Options

- `dsn`: Oracle connection string (host/service_name)
- `user`: Oracle username
- `password_env`: Environment variable containing Oracle password
- `extract_stored_procedures`: Enable stored procedure extraction
- `stored_proc_schemas`: List of schemas to extract from (empty = all)
- `stored_proc_patterns`: SQL LIKE patterns for procedure names
- `parse_dynamic_sql`: Parse EXECUTE IMMEDIATE statements
- `stored_proc_source`: Source view (all_source, dba_source, or user_source)

See [ORACLE_STORED_PROCS.md](../../docs/ORACLE_STORED_PROCS.md) for complete documentation.
