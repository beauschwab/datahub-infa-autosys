from __future__ import annotations

import os
from typing import Dict, Iterator, List

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.source import Source

from datahub_custom_sources.config import EssbaseSourceConfig
from datahub_custom_sources.emit.builders import (
    make_edge,
    mcp_dataflow_info,
    mcp_datajob_info,
    mcp_datajob_io,
    mcp_dataset_platform_instance,
    mcp_dataset_properties,
    mcp_schema_metadata,
)
from datahub_custom_sources.extractors.essbase_client import EssbaseClient, EssbaseConnectionConfig
from datahub_custom_sources.extractors.essbase_schema import SchemaExtractor
from datahub_custom_sources.sources.common import SimpleReport, as_workunits
from datahub_custom_sources.utils.urns import dataflow_urn, datajob_urn, dataset_urn


class EssbaseSource(Source):
    """
    DataHub ingestion Source plugin: Essbase REST metadata.

    Emits:
      - DataFlow per Essbase application
      - DataJob per cube (and optional calc scripts)
      - Dataset + schema per cube
    """

    source_config: EssbaseSourceConfig
    ctx: PipelineContext
    report: SimpleReport

    def __init__(self, ctx: PipelineContext, config: EssbaseSourceConfig):
        super().__init__(ctx)
        self.ctx = ctx
        self.source_config = config
        self.report = SimpleReport()

        password = os.getenv(config.password_env)
        if not password:
            raise ValueError(f"Essbase password env var not set: {config.password_env}")

        self.client = EssbaseClient(
            EssbaseConnectionConfig(
                base_url=config.base_url,
                username=config.user,
                password=password,
                verify_ssl=config.verify_ssl,
                timeout=config.timeout_s,
            )
        )
        self.schema_extractor = SchemaExtractor(
            self.client,
            include_members=config.include_members,
            include_formulas=config.include_formulas,
        )

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "EssbaseSource":
        config = EssbaseSourceConfig.model_validate(config_dict)
        return cls(ctx, config)

    def get_report(self) -> SimpleReport:
        return self.report

    def close(self) -> None:
        self.client.close()

    def get_workunits(self) -> Iterator:
        cfg = self.source_config
        mcps: List = []

        apps = cfg.applications or [a["name"] for a in self.client.list_applications()]

        for app_name in apps:
            flow_urn = dataflow_urn(cfg.dataflow_platform, app_name, cfg.dataflow_env)
            mcps.append(mcp_dataflow_info(flow_urn, name=app_name, description="Essbase application"))

            cubes = [c["name"] for c in self.client.list_cubes(app_name)]
            if cfg.cubes:
                cubes = [c for c in cubes if c in cfg.cubes]

            for cube_name in cubes:
                cube = self.schema_extractor.extract_cube(app_name, cube_name)
                ds_name = f"{app_name}.{cube_name}"
                ds_urn = dataset_urn(
                    cfg.dataset_platform,
                    ds_name,
                    cfg.dataset_env,
                    cfg.dataset_platform_instance,
                )

                custom_props = {
                    "essbase_application": app_name,
                    "essbase_cube": cube_name,
                    "dimension_count": str(len(cube.dimensions)),
                }
                if cube.number_of_blocks is not None:
                    custom_props["number_of_blocks"] = str(cube.number_of_blocks)
                if cube.block_density is not None:
                    custom_props["block_density"] = str(cube.block_density)

                mcps.append(mcp_dataset_properties(ds_urn, name=ds_name, custom_properties=custom_props))

                if cfg.dataset_platform_instance:
                    mcps.append(
                        mcp_dataset_platform_instance(
                            ds_urn, cfg.dataset_platform, cfg.dataset_platform_instance
                        )
                    )

                fields = self.schema_extractor.build_schema_fields(cube)
                if fields:
                    mcps.append(
                        mcp_schema_metadata(
                            ds_urn, cfg.dataset_platform, fields, schema_name=f"{ds_name}_schema"
                        )
                    )

                job_urn = datajob_urn(flow_urn, cube_name)
                mcps.append(
                    mcp_datajob_info(
                        job_urn=job_urn,
                        name=cube_name,
                        job_type="ESSBASE_CUBE",
                        flow_urn=flow_urn,
                        custom_properties=custom_props,
                    )
                )
                mcps.append(
                    mcp_datajob_io(
                        job_urn=job_urn,
                        input_datasets=[],
                        output_datasets=[ds_urn],
                        input_dataset_edges=None,
                        output_dataset_edges=[make_edge(ds_urn)],
                        input_datajob_edges=None,
                    )
                )

                if cfg.include_calc_scripts:
                    for script in self.client.list_calc_scripts(app_name, cube_name):
                        script_name = script.get("name") or "calc_script"
                        script_content = self.client.get_calc_script_content(
                            app_name, cube_name, script_name
                        )
                        if script_content and len(script_content) > cfg.calc_script_max_chars:
                            script_content = script_content[: cfg.calc_script_max_chars]

                        job_id = f"{cube_name}::{script_name}"
                        script_job_urn = datajob_urn(flow_urn, job_id)
                        mcps.append(
                            mcp_datajob_info(
                                job_urn=script_job_urn,
                                name=script_name,
                                job_type="ESSBASE_CALC_SCRIPT",
                                flow_urn=flow_urn,
                                custom_properties={
                                    "essbase_application": app_name,
                                    "essbase_cube": cube_name,
                                    "calc_script_name": script_name,
                                    "calc_script_content": script_content or "",
                                },
                            )
                        )
                        mcps.append(
                            mcp_datajob_io(
                                job_urn=script_job_urn,
                                input_datasets=[ds_urn],
                                output_datasets=[ds_urn],
                                input_dataset_edges=[make_edge(ds_urn)],
                                output_dataset_edges=[make_edge(ds_urn)],
                                input_datajob_edges=None,
                            )
                        )

                if cfg.include_load_rules:
                    for rule_type in ("data", "dimension"):
                        for rule in self.client.list_load_rules(app_name, cube_name, rule_type=rule_type):
                            rule_name = rule.get("name") or f"{rule_type}_rule"
                            rule_detail = self.client.get_load_rule(
                                app_name, cube_name, rule_name, rule_type=rule_type
                            )
                            rule_detail = rule_detail or {}

                            source = rule_detail.get("dataSource", {}) if isinstance(rule_detail, dict) else {}
                            source_name = source.get("name") or source.get("connection") or source.get("path")
                            source_type = source.get("type") or "unknown"
                            sql_text = source.get("sql") or source.get("query") or ""
                            if sql_text and len(sql_text) > cfg.load_rule_max_chars:
                                sql_text = sql_text[: cfg.load_rule_max_chars]

                            job_id = f"{cube_name}::{rule_type}::{rule_name}"
                            rule_job_urn = datajob_urn(flow_urn, job_id)
                            mcps.append(
                                mcp_datajob_info(
                                    job_urn=rule_job_urn,
                                    name=rule_name,
                                    job_type=f"ESSBASE_LOAD_RULE_{rule_type.upper()}",
                                    flow_urn=flow_urn,
                                    custom_properties={
                                        "essbase_application": app_name,
                                        "essbase_cube": cube_name,
                                        "rule_type": rule_type,
                                        "source_type": source_type,
                                        "source_name": source_name or "",
                                        "sql_preview": sql_text,
                                    },
                                )
                            )
                            mcps.append(
                                mcp_datajob_io(
                                    job_urn=rule_job_urn,
                                    input_datasets=[],
                                    output_datasets=[ds_urn],
                                    input_dataset_edges=None,
                                    output_dataset_edges=[make_edge(ds_urn)],
                                    input_datajob_edges=None,
                                )
                            )

        for wu in as_workunits("essbase", mcps):
            self.report.report_workunit()
            yield wu