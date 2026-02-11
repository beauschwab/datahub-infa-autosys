# Source Plugins

datahub-custom-sources ships **six DataHub ingestion source plugins**, each registered as a `datahub ingest` entry-point. Every source follows the same pattern:

1. Parse artifacts from the source system (CLI exports, XML files, REST APIs, text files)
2. Build DataHub entities using shared [MCP builders](../api-reference.md)
3. Emit `MetadataChangeProposalWrapper` work units to the configured sink

## At a Glance

| Plugin name          | Source class                | Connectivity       | Key artifacts |
|----------------------|-----------------------------|---------------------|---------------|
| `infa_pmrep`         | `InformaticaPmrepSource`    | pmrep CLI + XML     | Folder exports |
| `autosys_jil`        | `AutoSysJilSource`          | JIL text files      | `.jil` exports |
| `oracle_operational` | `OracleOperationalSource`   | Oracle DB (oracledb)| V$SQL, audit trail |
| `essbase`            | `EssbaseSource`             | REST API            | Cubes, calc scripts, load rules |
| `ssis_dtsx`          | `SsisDtsxSource`            | File-based          | `.dtsx` XML packages |
| `abinitio`           | `AbInitioSource`            | File-based          | `.mp`/`.g` graphs, `.dml`, `.xfr` |

## Shared Configuration

All sources that emit dataset lineage use `CommonLineageConfig`:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `platform` | `str` | `"oracle"` | DataHub platform name for physical datasets |
| `platform_instance` | `str?` | `null` | Optional DataHub platform instance |
| `env` | `str` | `"PROD"` | DataHub environment / fabric |
| `default_db` | `str?` | `null` | Default database for SQL parsing |
| `default_schema` | `str?` | `null` | Default schema for SQL parsing |
| `ignore_table_patterns` | `list[str]` | `[]` | SQL LIKE patterns for tables to ignore |
| `custom_properties` | `dict` | `{}` | Extra properties to attach to entities |

## DataHub Entity Model

All sources emit entities using this hierarchy:

```
DataFlow (container — folder, box, app, package, project)
  └── DataJob (unit of work — workflow, job, calc script, task, graph)
        ├── inputDatasets[]   — URNs of datasets read
        └── outputDatasets[]  — URNs of datasets written

Dataset (table, file, cube)
  ├── SchemaMetadata (columns/fields)
  └── UpstreamLineage (dataset → dataset edges, optionally column-level)
```

!!! tip "Lineage convention"
    Field/column-level lineage is emitted primarily using **Dataset-level** `UpstreamLineage` aspects, because DataHub UI support is strongest there. Job-level fine-grained lineage exists in the model but is not universally rendered.
