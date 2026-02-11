from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterator, List


from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.source import Source

from datahub_custom_sources.config import SsisSourceConfig
from datahub_custom_sources.emit.builders import (
    make_edge,
    mcp_dataflow_info,
    mcp_datajob_info,
    mcp_datajob_io,
    mcp_dataset_platform_instance,
    mcp_dataset_properties,
    mcp_upstream_lineage,
)
from datahub_custom_sources.extractors.ssis_dtsx import (
    extract_select_column_lineage,
    extract_sql_tables,
    load_schema_mapping,
    parse_dtsx,
)
from datahub_custom_sources.sources.common import SimpleReport, as_workunits
from datahub_custom_sources.utils.paths import expand_paths
from datahub_custom_sources.utils.urns import dataflow_urn, datajob_urn, dataset_urn


class SsisDtsxSource(Source):
    """
    DataHub ingestion Source plugin: SSIS DTSX packages.

    Emits:
      - DataFlow per package
      - DataJob per task
      - Dataset lineage from SQL statements (best-effort)
    """

    source_config: SsisSourceConfig
    ctx: PipelineContext
    report: SimpleReport

    def __init__(self, ctx: PipelineContext, config: SsisSourceConfig):
        super().__init__(ctx)
        self.ctx = ctx
        self.source_config = config
        self.report = SimpleReport()

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "SsisDtsxSource":
        config = SsisSourceConfig.model_validate(config_dict)
        return cls(ctx, config)

    def get_report(self) -> SimpleReport:
        return self.report

    def close(self) -> None:
        pass

    def get_workunits(self) -> Iterator:
        cfg = self.source_config
        mcps: List = []

        schema_mapping = load_schema_mapping(cfg.schema_paths) if cfg.schema_paths else {}

        for path in expand_paths(cfg.dtsx_paths, cfg.dtsx_dirs, [".dtsx"]):
            package = parse_dtsx(path)
            flow_id = package.name or Path(path).stem
            flow_urn = dataflow_urn(cfg.dataflow_platform, flow_id, cfg.dataflow_env)
            mcps.append(mcp_dataflow_info(flow_urn, name=flow_id, description="SSIS package"))

            for task in package.tasks:
                job_urn = datajob_urn(flow_urn, task.name)
                custom_props = {
                    "ssis_task_type": task.task_type,
                    "package_path": str(package.path),
                }

                sql_preview = "\n".join(task.sql_statements)
                if sql_preview:
                    if len(sql_preview) > cfg.sql_max_chars:
                        sql_preview = sql_preview[: cfg.sql_max_chars]
                    custom_props["sql_preview"] = sql_preview

                mcps.append(
                    mcp_datajob_info(
                        job_urn=job_urn,
                        name=task.name,
                        job_type=f"SSIS_{task.task_type.upper()}",
                        flow_urn=flow_urn,
                        custom_properties=custom_props,
                    )
                )

                input_datasets: list[str] = []
                output_datasets: list[str] = []
                dataset_names: dict[str, str] = {}

                if cfg.parse_sql:
                    for sql in task.sql_statements:
                        inputs, outputs = extract_sql_tables(
                            sql,
                            default_db=cfg.lineage.default_db,
                            default_schema=cfg.lineage.default_schema,
                        )
                        for table in inputs:
                            urn = dataset_urn(
                                cfg.lineage.platform,
                                table,
                                cfg.lineage.env,
                                cfg.lineage.platform_instance,
                            )
                            input_datasets.append(urn)
                            dataset_names[urn] = table
                        for table in outputs:
                            urn = dataset_urn(
                                cfg.lineage.platform,
                                table,
                                cfg.lineage.env,
                                cfg.lineage.platform_instance,
                            )
                            output_datasets.append(urn)
                            dataset_names[urn] = table

                input_datasets = sorted(set(input_datasets))
                output_datasets = sorted(set(output_datasets))

                for ds in input_datasets + output_datasets:
                    mcps.append(mcp_dataset_properties(ds, name=dataset_names.get(ds, ds)))
                    if cfg.lineage.platform_instance:
                        mcps.append(
                            mcp_dataset_platform_instance(
                                ds, cfg.lineage.platform, cfg.lineage.platform_instance
                            )
                        )

                if input_datasets or output_datasets:
                    mcps.append(
                        mcp_datajob_io(
                            job_urn=job_urn,
                            input_datasets=input_datasets,
                            output_datasets=output_datasets,
                            input_dataset_edges=[make_edge(d) for d in input_datasets] or None,
                            output_dataset_edges=[make_edge(d) for d in output_datasets] or None,
                            input_datajob_edges=None,
                        )
                    )

                if input_datasets and output_datasets:
                    for output_ds in output_datasets:
                        mcps.append(mcp_upstream_lineage(output_ds, input_datasets))

                if input_datasets and output_datasets and cfg.parse_sql and task.sql_statements:
                    table_to_urn = {name: urn for urn, name in dataset_names.items()}
                    output_ds = output_datasets[0] if output_datasets else None
                    fine_grained = []

                    for sql in task.sql_statements:
                        mappings = extract_select_column_lineage(
                            sql,
                            default_db=cfg.lineage.default_db,
                            default_schema=cfg.lineage.default_schema,
                        )
                        for output_col, upstream_cols in mappings:
                            upstream_field_urns: list[str] = []
                            for table_name, col_name in upstream_cols:
                                if table_name:
                                    ds_urn = table_to_urn.get(table_name)
                                else:
                                    ds_urn = input_datasets[0] if len(input_datasets) == 1 else None
                                if not ds_urn:
                                    continue
                                upstream_field_urns.append(make_schema_field_urn(ds_urn, col_name))

                            if not upstream_field_urns or not output_ds:
                                continue
                            downstream_field_urns = [make_schema_field_urn(output_ds, output_col)]
                            fine_grained.append(("SQL_SELECT", upstream_field_urns, downstream_field_urns, sql))

                    if fine_grained and output_ds:
                        mcps.append(mcp_upstream_lineage(output_ds, input_datasets, fine_grained))

        for wu in as_workunits("ssis", mcps):
            self.report.report_workunit()
            yield wu