from __future__ import annotations

from typing import Dict, List, Optional

from pydantic import Field
from datahub.api.configuration.common import ConfigModel


class CommonLineageConfig(ConfigModel):
    """
    Shared lineage options for Informatica + Oracle + AutoSys emitted datasets.
    """

    platform: str = Field(
        default="oracle",
        description="DataHub platform name to use for physical datasets (e.g. oracle).",
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="Optional DataHub platform instance (e.g. PROD).",
    )
    env: str = Field(
        default="PROD",
        description="DataHub environment / fabric (commonly PROD/DEV).",
    )
    default_db: Optional[str] = Field(
        default=None, description="Default database to assume when parsing SQL."
    )
    default_schema: Optional[str] = Field(
        default=None, description="Default schema to assume when parsing SQL."
    )
    ignore_table_patterns: List[str] = Field(
        default_factory=list,
        description="List of SQL LIKE patterns for placeholder/dummy tables to ignore (e.g. DUMMY_%).",
    )
    custom_properties: Dict[str, str] = Field(
        default_factory=dict,
        description="Custom properties to attach to emitted entities.",
    )


class PmrepConfig(ConfigModel):
    bin_path: str = Field(
        description="Absolute path to pmrep binary, e.g. /opt/informatica/server/bin/pmrep"
    )
    domain: str = Field(description="Informatica domain name used by pmrep.")
    repo: str = Field(description="Informatica repository name.")
    user: str = Field(description="Informatica repository user name.")
    password_env: str = Field(
        description="Environment variable name containing Informatica password."
    )
    connect_timeout_s: int = Field(default=60, description="Timeout for pmrep connect.")
    extra_env: Dict[str, str] = Field(
        default_factory=dict,
        description="Extra environment variables to pass to pmrep subprocess.",
    )


class InformaticaExportConfig(ConfigModel):
    folder: str = Field(description="Informatica folder/project to export.")
    out_dir: str = Field(description="Directory to write exported XML artifacts.")
    include_workflows: bool = Field(default=True, description="Export workflows.")
    include_mappings: bool = Field(default=True, description="Export mappings.")
    overwrite: bool = Field(default=True, description="Overwrite existing exported files.")
    object_name_filter: Optional[str] = Field(
        default=None,
        description="Optional name filter (regex) for exported objects.",
    )


class InformaticaLineageConfig(ConfigModel):
    """
    Informatica -> dataset lineage behavior.
    """

    lineage: CommonLineageConfig = Field(default_factory=CommonLineageConfig)
    infer_from_sql: bool = Field(
        default=True,
        description="Attempt to infer dataset + column lineage from embedded SQL snippets (SQL overrides, expressions, stored proc calls).",
    )
    infer_from_mapping_ports: bool = Field(
        default=True,
        description="Compute column lineage by traversing Informatica mapping ports/connectors when possible.",
    )
    attach_transformation_text: bool = Field(
        default=True,
        description="Attach raw SQL / expressions as transformation text via custom properties.",
    )
    max_sql_snippet_chars: int = Field(
        default=20000,
        description="Hard cap on stored SQL snippet length to avoid oversized events.",
    )


class InformaticaSourceConfig(ConfigModel):
    pmrep: PmrepConfig
    export: InformaticaExportConfig
    lineage: InformaticaLineageConfig = Field(default_factory=InformaticaLineageConfig)

    dataflow_platform: str = Field(
        default="informatica",
        description="Platform name to use for DataFlow/DataJob templates representing Informatica objects.",
    )
    dataflow_env: str = Field(
        default="PROD",
        description="Environment (fabric) for Informatica DataFlow/DataJob URNs.",
    )
    treat_workflow_as_dataflow: bool = Field(
        default=False,
        description="If true, model each workflow as a DataFlow and sessions/mappings as DataJobs under it. If false, folder is DataFlow and workflow is DataJob.",
    )


class AutoSysSourceConfig(ConfigModel):
    platform: str = Field(
        default="autosys",
        description="Platform name to use for AutoSys DataFlows/DataJobs.",
    )
    env: str = Field(default="PROD", description="Environment for AutoSys URNs.")
    jil_paths: List[str] = Field(
        description="Paths to .jil files (or exported JIL text) to parse."
    )
    custom_properties: Dict[str, str] = Field(
        default_factory=dict,
        description="Custom properties to attach to emitted entities.",
    )

    bridge_to_informatica: bool = Field(
        default=True,
        description="If true, create job->job edges from AutoSys jobs that execute Informatica workflows to the corresponding Informatica DataJobs.",
    )
    informatica_job_name_regex: str = Field(
        default=r"(?i)\b(infa|informatica)\b.*\b(?P<wf>[A-Z0-9_\-\.]+)\b",
        description="Regex used to detect an Informatica workflow name inside an AutoSys command.",
    )
    informatica_dataflow_platform: str = Field(
        default="informatica",
        description="Platform used when creating URNs for Informatica DataJobs in bridges.",
    )
    informatica_dataflow_env: str = Field(
        default="PROD",
        description="Environment used for Informatica URNs in bridges.",
    )
    informatica_dataflow_id: str = Field(
        default="default",
        description="DataFlow id used when creating Informatica job URNs in bridges (folder/workspace).",
    )


class EssbaseSourceConfig(ConfigModel):
    base_url: str = Field(description="Essbase REST API base URL, e.g. https://essbase.company.com")
    user: str = Field(description="Essbase username")
    password_env: str = Field(description="Env var containing Essbase password")
    verify_ssl: bool = Field(default=True, description="Verify SSL certificates for Essbase REST calls.")
    timeout_s: int = Field(default=30, description="HTTP timeout in seconds for Essbase REST calls.")

    applications: Optional[List[str]] = Field(
        default=None,
        description="Optional list of Essbase applications to extract (default: all).",
    )
    cubes: Optional[List[str]] = Field(
        default=None,
        description="Optional list of cube names to extract within selected applications.",
    )

    include_members: bool = Field(default=True, description="Include dimension members in schema extraction.")
    include_formulas: bool = Field(default=True, description="Fetch member formulas when available.")
    include_calc_scripts: bool = Field(default=True, description="Emit calc scripts as jobs.")
    include_load_rules: bool = Field(default=True, description="Emit load rules as jobs.")
    calc_script_max_chars: int = Field(
        default=20000,
        description="Max chars of calc script content to store in job custom properties.",
    )
    load_rule_max_chars: int = Field(
        default=20000,
        description="Max chars of load rule SQL text to store in job custom properties.",
    )

    dataflow_platform: str = Field(
        default="essbase",
        description="Platform name to use for Essbase DataFlows/DataJobs.",
    )
    dataflow_env: str = Field(
        default="PROD",
        description="Environment for Essbase DataFlow/DataJob URNs.",
    )

    dataset_platform: str = Field(
        default="essbase",
        description="Platform name to use for Essbase datasets.",
    )
    dataset_platform_instance: Optional[str] = Field(
        default=None,
        description="Optional platform instance for Essbase datasets.",
    )
    dataset_env: str = Field(default="PROD", description="Environment for Essbase datasets.")


class SsisSourceConfig(ConfigModel):
    dtsx_paths: List[str] = Field(description="Paths to SSIS .dtsx package files.")
    dtsx_dirs: List[str] = Field(
        default_factory=list,
        description="Directories to search recursively for .dtsx packages.",
    )
    schema_paths: List[str] = Field(
        default_factory=list,
        description="JSON files mapping table names to column lists for column lineage.",
    )
    dataflow_platform: str = Field(
        default="ssis",
        description="Platform name to use for SSIS DataFlows/DataJobs.",
    )
    dataflow_env: str = Field(default="PROD", description="Environment for SSIS DataFlow/DataJob URNs.")

    lineage: CommonLineageConfig = Field(default_factory=CommonLineageConfig)
    parse_sql: bool = Field(
        default=True,
        description="If true, extract dataset lineage from SQL found in packages.",
    )
    sql_max_chars: int = Field(
        default=20000,
        description="Max chars of SQL text to store in job custom properties.",
    )


class AbInitioSourceConfig(ConfigModel):
    graph_paths: List[str] = Field(
        description="Paths to Ab Initio graph files (e.g., .mp, .g, exported XML).",
    )
    graph_dirs: List[str] = Field(
        default_factory=list,
        description="Directories to search recursively for graph files.",
    )
    dml_paths: List[str] = Field(
        default_factory=list,
        description="Paths to DML schema files to emit dataset schemas.",
    )
    dml_dirs: List[str] = Field(
        default_factory=list,
        description="Directories to search recursively for DML schema files.",
    )
    xfr_paths: List[str] = Field(
        default_factory=list,
        description="Paths to XFR transform files for field-level mappings (optional).",
    )
    xfr_dirs: List[str] = Field(
        default_factory=list,
        description="Directories to search recursively for XFR transform files.",
    )
    io_mapping_paths: List[str] = Field(
        default_factory=list,
        description="JSON files mapping graph names to input/output datasets.",
    )
    project_name: Optional[str] = Field(
        default=None,
        description="Optional Ab Initio project name used for DataFlow ids.",
    )

    dataflow_platform: str = Field(
        default="abinitio",
        description="Platform name to use for Ab Initio DataFlows/DataJobs.",
    )
    dataflow_env: str = Field(default="PROD", description="Environment for Ab Initio URNs.")

    lineage: CommonLineageConfig = Field(default_factory=CommonLineageConfig)
    emit_schema_from_dml: bool = Field(
        default=True,
        description="If true, emit dataset schema metadata from DML files.",
    )


class OracleOperationalConfig(ConfigModel):
    """
    Operational lineage for Oracle.

    The intent is to record what *actually happened* in a run: which datasets were
    read/written, which partitions/predicates were used, timings, outcomes, etc.
    This is typically emitted as DataProcessInstance.
    """

    dsn: str = Field(description="Oracle DSN, e.g. host/service_name or a full connect descriptor.")
    user: str = Field(description="Oracle username.")
    password_env: str = Field(description="Env var containing Oracle password.")

    poll_interval_s: int = Field(default=60, description="Polling interval for pulling new run/usage records.")
    lookback_minutes: int = Field(default=180, description="On startup, look back this many minutes for runs.")

    lineage: CommonLineageConfig = Field(default_factory=CommonLineageConfig)
    emit_queries: bool = Field(
        default=False,
        description="If true, also emit Query entities + inferred SQL lineage from captured statements.",
    )
    emit_instances: bool = Field(
        default=True,
        description="If true, emit DataProcessInstance entities for runs.",
    )
    query_sources: List[str] = Field(
        default_factory=lambda: ["v$sql"],
        description="Which Oracle sources to read queries from (e.g. v$sql, dba_hist_sqltext, unified_audit_trail).",
    )
