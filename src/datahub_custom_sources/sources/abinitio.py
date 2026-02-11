from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterator, List

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.source import Source

from datahub_custom_sources.config import AbInitioSourceConfig
from datahub_custom_sources.emit.builders import (
    make_edge,
    mcp_dataflow_info,
    mcp_datajob_info,
    mcp_datajob_io,
    mcp_dataset_platform_instance,
    mcp_dataset_properties,
    mcp_schema_metadata,
    mcp_upstream_lineage,
)
from datahub_custom_sources.extractors.abinitio import (
    load_io_mapping,
    parse_dml,
    parse_xfr_mappings,
)
from datahub_custom_sources.sources.common import SimpleReport, as_workunits
from datahub_custom_sources.utils.paths import expand_paths
from datahub_custom_sources.utils.urns import dataflow_urn, datajob_urn, dataset_urn
from datahub.metadata.schema_classes import SchemaFieldClass, SchemaFieldDataTypeClass, StringTypeClass


class AbInitioSource(Source):
    """
    DataHub ingestion Source plugin: Ab Initio graphs + DML/XFR.

    Emits:
      - DataFlow per project (or directory)
      - DataJob per graph
      - Dataset + schema per DML file
      - Optional dataset lineage from XFR (best-effort)
    """

    source_config: AbInitioSourceConfig
    ctx: PipelineContext
    report: SimpleReport

    def __init__(self, ctx: PipelineContext, config: AbInitioSourceConfig):
        super().__init__(ctx)
        self.ctx = ctx
        self.source_config = config
        self.report = SimpleReport()

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "AbInitioSource":
        config = AbInitioSourceConfig.model_validate(config_dict)
        return cls(ctx, config)

    def get_report(self) -> SimpleReport:
        return self.report

    def close(self) -> None:
        pass

    def get_workunits(self) -> Iterator:
        cfg = self.source_config
        mcps: List = []

        io_mapping = load_io_mapping(cfg.io_mapping_paths) if cfg.io_mapping_paths else {}

        flow_id = cfg.project_name or "abinitio"
        flow_urn = dataflow_urn(cfg.dataflow_platform, flow_id, cfg.dataflow_env)
        mcps.append(mcp_dataflow_info(flow_urn, name=flow_id, description="Ab Initio project"))

        dml_schemas: dict[str, str] = {}
        for dml_path in expand_paths(cfg.dml_paths, cfg.dml_dirs, [".dml"]):
            schema = parse_dml(dml_path)
            ds_name = schema.name
            ds_urn = dataset_urn(
                cfg.lineage.platform,
                ds_name,
                cfg.lineage.env,
                cfg.lineage.platform_instance,
            )
            dml_schemas[schema.name] = ds_urn

            mcps.append(mcp_dataset_properties(ds_urn, name=ds_name))
            if cfg.lineage.platform_instance:
                mcps.append(
                    mcp_dataset_platform_instance(
                        ds_urn, cfg.lineage.platform, cfg.lineage.platform_instance
                    )
                )

            if cfg.emit_schema_from_dml and schema.fields:
                fields = [
                    SchemaFieldClass(
                        fieldPath=f.name,
                        type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                        nativeDataType=f.field_type,
                    )
                    for f in schema.fields
                ]
                mcps.append(mcp_schema_metadata(ds_urn, cfg.lineage.platform, fields, schema_name=ds_name))

        for graph_path in expand_paths(cfg.graph_paths, cfg.graph_dirs, [".mp", ".g", ".xml"]):
            job_name = graph_path.stem
            job_urn = datajob_urn(flow_urn, job_name)
            mcps.append(
                mcp_datajob_info(
                    job_urn=job_urn,
                    name=job_name,
                    job_type="ABINITIO_GRAPH",
                    flow_urn=flow_urn,
                    custom_properties={"graph_path": str(graph_path)},
                )
            )
            if job_name in io_mapping:
                inputs = [
                    dataset_urn(
                        cfg.lineage.platform,
                        ds,
                        cfg.lineage.env,
                        cfg.lineage.platform_instance,
                    )
                    for ds in io_mapping[job_name].get("inputs", [])
                ]
                outputs = [
                    dataset_urn(
                        cfg.lineage.platform,
                        ds,
                        cfg.lineage.env,
                        cfg.lineage.platform_instance,
                    )
                    for ds in io_mapping[job_name].get("outputs", [])
                ]
                inputs = sorted(set(inputs))
                outputs = sorted(set(outputs))

                mcps.append(
                    mcp_datajob_io(
                        job_urn=job_urn,
                        input_datasets=inputs,
                        output_datasets=outputs,
                        input_dataset_edges=[make_edge(d) for d in inputs] or None,
                        output_dataset_edges=[make_edge(d) for d in outputs] or None,
                        input_datajob_edges=None,
                    )
                )

                if inputs and outputs:
                    for output_ds in outputs:
                        mcps.append(mcp_upstream_lineage(output_ds, inputs))
            else:
                mcps.append(
                    mcp_datajob_io(
                        job_urn=job_urn,
                        input_datasets=[],
                        output_datasets=[],
                        input_dataset_edges=None,
                        output_dataset_edges=None,
                        input_datajob_edges=None,
                    )
                )

        expanded_xfr = expand_paths(cfg.xfr_paths, cfg.xfr_dirs, [".xfr"])

        if expanded_xfr and len(dml_schemas) >= 2:
            # Best-effort: assume first schema is input and second is output for mappings
            schema_names = list(dml_schemas.keys())
            input_ds = dml_schemas[schema_names[0]]
            output_ds = dml_schemas[schema_names[1]]

            for xfr_path in expanded_xfr:
                mappings = parse_xfr_mappings(xfr_path)
                if not mappings:
                    continue

                fine_grained = [
                    ("XFR", [src], [dst], None)
                    for src, dst in mappings
                ]
                mcps.append(mcp_upstream_lineage(output_ds, [input_ds], fine_grained))

        for wu in as_workunits("abinitio", mcps):
            self.report.report_workunit()
            yield wu