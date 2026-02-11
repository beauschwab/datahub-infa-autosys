from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.source import Source

from datahub_custom_sources.config import InformaticaSourceConfig
from datahub_custom_sources.emit.builders import (
    make_edge,
    mcp_dataflow_info,
    mcp_datajob_info,
    mcp_datajob_io,
    mcp_dataset_platform_instance,
    mcp_upstream_lineage,
)
from datahub_custom_sources.extractors.informatica_xml import (
    RepoExport,
    TableRef,
    parse_informatica_folder_xml,
    resolve_upstream_source_fields,
)
from datahub_custom_sources.extractors.pmrep import PmrepRunner
from datahub_custom_sources.sources.common import SimpleReport, as_workunits
from datahub_custom_sources.utils.urns import dataflow_urn, datajob_urn, dataset_urn


def _like_to_regex(pat: str) -> re.Pattern[str]:
    # SQL LIKE: % -> .*, _ -> .
    esc = re.escape(pat).replace(r"\%", ".*").replace(r"\_", ".")
    return re.compile(f"^{esc}$", re.I)


class InformaticaPmrepSource(Source):
    """
    DataHub ingestion Source plugin: Informatica via pmrep export + XML parsing.

    Emits:
      - DataFlow: folder (or workflows if treat_workflow_as_dataflow)
      - DataJobs: workflows and/or mappings
      - DataJobInputOutput for dataset edges
      - Dataset UpstreamLineage (coarse + fine-grained column lineage when inferable)
    """

    source_config: InformaticaSourceConfig
    ctx: PipelineContext
    report: SimpleReport

    def __init__(self, ctx: PipelineContext, config: InformaticaSourceConfig):
        super().__init__(ctx)
        self.ctx = ctx
        self.source_config = config
        self.report = SimpleReport()
        self._ignore_table_res = [_like_to_regex(p) for p in config.lineage.lineage.ignore_table_patterns]

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "InformaticaPmrepSource":
        # Pydantic v2
        config = InformaticaSourceConfig.model_validate(config_dict)
        return cls(ctx, config)

    def get_report(self) -> SimpleReport:
        return self.report

    def close(self) -> None:
        pass

    # -------- internals --------

    def _is_ignored_table(self, table_name: str) -> bool:
        return any(r.match(table_name) for r in self._ignore_table_res)

    def _table_to_dataset_urn(self, t: TableRef) -> Optional[str]:
        fqn = t.fqn(
            default_db=self.source_config.lineage.lineage.default_db,
            default_schema=self.source_config.lineage.lineage.default_schema,
        )
        # Treat unqualified names as still valid; ignore dummy placeholders via pattern match
        if self._is_ignored_table(t.name) or self._is_ignored_table(fqn):
            return None
        lc = self.source_config.lineage.lineage
        return dataset_urn(
            platform=lc.platform,
            name=fqn,
            env=lc.env,
            platform_instance=lc.platform_instance,
        )

    def _emit_platform_instance(self, ds_urn: str) -> Iterable:
        lc = self.source_config.lineage.lineage
        if lc.platform_instance:
            yield mcp_dataset_platform_instance(ds_urn, platform=lc.platform, instance=lc.platform_instance)

    def _job_custom_props(self, sql_snippets: Sequence[str], expressions: Sequence[str]) -> Dict[str, str]:
        cfg = self.source_config.lineage
        props: Dict[str, str] = dict(cfg.lineage.custom_properties)
        props["sql_snippet_count"] = str(len(sql_snippets))
        props["expression_count"] = str(len(expressions))

        if cfg.attach_transformation_text and sql_snippets:
            joined = "\n\n-- ---\n\n".join(s[: cfg.max_sql_snippet_chars] for s in sql_snippets)
            # DataHub customProperties values should stay reasonably small; store a hash + first chunk.
            sha = hashlib.sha256(joined.encode("utf-8")).hexdigest()
            props["sql_snippets_sha256"] = sha
            props["sql_snippets_preview"] = joined[: min(len(joined), 4000)]
        return props

    # -------- Source API --------

    def get_workunits(self) -> Iterator:
        cfg = self.source_config

        # 1) export via pmrep (or reuse existing)
        runner = PmrepRunner(cfg.pmrep)
        xml_path = runner.export_folder(cfg.export)
        export = parse_informatica_folder_xml(xml_path, folder=cfg.export.folder)

        # 2) DataFlow(s)
        folder_flow_urn = dataflow_urn(cfg.dataflow_platform, cfg.export.folder, cfg.dataflow_env)
        mcps = [mcp_dataflow_info(folder_flow_urn, name=cfg.export.folder, description="Informatica folder")]

        # 3) Emit workflows as DataJobs (lightweight)
        for wf in export.workflows.values():
            wf_job_urn = datajob_urn(folder_flow_urn, wf.name)
            mcps.append(
                mcp_datajob_info(
                    job_urn=wf_job_urn,
                    name=wf.name,
                    job_type="INFORMATICA_WORKFLOW",
                    flow_urn=folder_flow_urn,
                    custom_properties=self._job_custom_props(wf.sql_snippets, []),
                )
            )

        # 4) Emit mappings as DataJobs + I/O + dataset lineage
        for mapping in export.mappings.values():
            mcps.extend(self._emit_mapping(folder_flow_urn, mapping))

        # Convert to workunits
        for wu in as_workunits("infa", mcps):
            self.report.report_workunit()
            yield wu

    def _emit_mapping(self, flow_urn: str, mapping) -> List:
        cfg = self.source_config
        lc = cfg.lineage.lineage
        out: List = []

        job_urn = datajob_urn(flow_urn, mapping.name)
        out.append(
            mcp_datajob_info(
                job_urn=job_urn,
                name=mapping.name,
                job_type="INFORMATICA_MAPPING",
                flow_urn=flow_urn,
                custom_properties=self._job_custom_props(mapping.sql_snippets, mapping.expressions),
            )
        )

        # datasets
        input_ds = [self._table_to_dataset_urn(t) for t in mapping.sources.values()]
        output_ds = [self._table_to_dataset_urn(t) for t in mapping.targets.values()]
        input_ds = [d for d in input_ds if d]
        output_ds = [d for d in output_ds if d]

        # platform instance aspects for datasets
        for ds in set(input_ds + output_ds):
            out.extend(list(self._emit_platform_instance(ds)))

        in_edges = [make_edge(d) for d in input_ds]
        out_edges = [make_edge(d) for d in output_ds]

        out.append(
            mcp_datajob_io(
                job_urn=job_urn,
                input_datasets=input_ds,
                output_datasets=output_ds,
                input_dataset_edges=in_edges or None,
                output_dataset_edges=out_edges or None,
                input_datajob_edges=None,
            )
        )

        # column lineage: mapping ports/connectors
        if cfg.lineage.infer_from_mapping_ports and input_ds and output_ds:
            out.extend(self._emit_mapping_column_lineage(mapping, input_ds, output_ds))

        return out

    def _emit_mapping_column_lineage(self, mapping, input_ds: Sequence[str], output_ds: Sequence[str]) -> List:
        """
        Emit dataset UpstreamLineage for each target dataset.
        """
        from datahub.emitter.mce_builder import make_schema_field_urn

        lc = self.source_config.lineage.lineage
        out: List = []

        # index instance->dataset urn
        src_inst_to_ds: Dict[str, str] = {}
        for inst, tref in mapping.sources.items():
            ds = self._table_to_dataset_urn(tref)
            if ds:
                src_inst_to_ds[inst] = ds

        tgt_inst_to_ds: Dict[str, str] = {}
        for inst, tref in mapping.targets.items():
            ds = self._table_to_dataset_urn(tref)
            if ds:
                tgt_inst_to_ds[inst] = ds

        # For each target column, compute upstream columns
        for tgt_inst, tgt_ds in tgt_inst_to_ds.items():
            fine = []
            # candidates: ports belonging to this target instance
            for (inst, field), port in mapping.ports.items():
                if inst != tgt_inst:
                    continue
                # In Informatica, PORTTYPE might not align perfectly; we just compute for any named port.
                ups = resolve_upstream_source_fields(mapping, target_instance=inst, target_field=field)
                upstream_field_urns: List[str] = []
                for src_inst, src_field in ups:
                    ds_urn = src_inst_to_ds.get(src_inst)
                    if not ds_urn:
                        continue
                    upstream_field_urns.append(make_schema_field_urn(ds_urn, src_field))
                if not upstream_field_urns:
                    continue
                downstream_field_urn = make_schema_field_urn(tgt_ds, field)
                fine.append((
                    "INFORMATICA_EXPRESSION",
                    upstream_field_urns,
                    [downstream_field_urn],
                    None,
                ))

            # Coarse upstreams = all input datasets
            out.append(
                mcp_upstream_lineage(
                    downstream_dataset_urn=tgt_ds,
                    upstream_dataset_urns=list(set(input_ds)),
                    fine_grained_field_mappings=fine or None,
                    upstream_properties={"source": "informatica_pmrep", "folder": self.source_config.export.folder},
                )
            )

        return out
