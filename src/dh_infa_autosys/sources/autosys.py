from __future__ import annotations

import re
from typing import Dict, Iterator, List, Optional

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.source import Source

from dh_infa_autosys.config import AutoSysSourceConfig
from dh_infa_autosys.emit.builders import make_edge, mcp_dataflow_info, mcp_datajob_info, mcp_datajob_io
from dh_infa_autosys.extractors.autosys_jil import parse_jil_files
from dh_infa_autosys.sources.common import SimpleReport, as_workunits
from dh_infa_autosys.utils.urns import dataflow_urn, datajob_urn


class AutoSysJilSource(Source):
    """
    DataHub ingestion Source plugin: AutoSys orchestration definitions via JIL.

    Emits:
      - DataFlow per AutoSys box
      - DataJob per AutoSys job
      - inputDatajobEdges expressing job dependencies from AutoSys `condition:` expressions

    Bridging:
      - If enabled, we attempt to detect Informatica workflow names inside job commands and store
        the target Informatica job URN as a custom property on the AutoSys DataJob.
    """

    source_config: AutoSysSourceConfig
    ctx: PipelineContext
    report: SimpleReport

    def __init__(self, ctx: PipelineContext, config: AutoSysSourceConfig):
        super().__init__(ctx)
        self.ctx = ctx
        self.source_config = config
        self.report = SimpleReport()
        self._wf_re = re.compile(config.informatica_job_name_regex)

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "AutoSysJilSource":
        config = AutoSysSourceConfig.model_validate(config_dict)
        return cls(ctx, config)

    def get_report(self) -> SimpleReport:
        return self.report

    def close(self) -> None:
        pass

    def get_workunits(self) -> Iterator:
        cfg = self.source_config
        exp = parse_jil_files(cfg.jil_paths)

        mcps: List = []

        # One DataFlow per box. Jobs not in a box go into a synthetic flow.
        synthetic_flow_id = "__autosys__"
        synthetic_flow_urn = dataflow_urn(cfg.platform, synthetic_flow_id, cfg.env)
        mcps.append(mcp_dataflow_info(synthetic_flow_urn, name=synthetic_flow_id, description="AutoSys standalone jobs"))

        box_flow_urns: Dict[str, str] = {}
        for box_name, box in exp.boxes.items():
            flow_urn = dataflow_urn(cfg.platform, box_name, cfg.env)
            box_flow_urns[box_name] = flow_urn
            mcps.append(mcp_dataflow_info(flow_urn, name=box_name, description="AutoSys box"))

        # Emit all jobs
        for job_name, job in exp.jobs.items():
            flow_urn = box_flow_urns.get(job.box_name) if job.box_name else synthetic_flow_urn
            job_urn = datajob_urn(flow_urn, job_name)

            props = dict(cfg.custom_properties)
            if job.command:
                props["command"] = job.command[:4000]

            # Bridge to Informatica if possible
            if cfg.bridge_to_informatica and job.command:
                m = self._wf_re.search(job.command)
                if m and m.groupdict().get("wf"):
                    wf = m.group("wf")
                    # URN that matches the Informatica source defaults (folder-level DataFlow).
                    infa_flow_urn = dataflow_urn(
                        cfg.informatica_dataflow_platform,
                        cfg.informatica_dataflow_id,
                        cfg.informatica_dataflow_env,
                    )
                    props["executes_informatica_job_urn"] = datajob_urn(infa_flow_urn, wf)

            mcps.append(
                mcp_datajob_info(
                    job_urn=job_urn,
                    name=job_name,
                    job_type=f"AUTOSYS_{job.job_type.upper()}",
                    flow_urn=flow_urn,
                    custom_properties=props or None,
                )
            )

        # Emit job->job edges via condition parsing
        for job_name, job in exp.jobs.items():
            flow_urn = box_flow_urns.get(job.box_name) if job.box_name else synthetic_flow_urn
            job_urn = datajob_urn(flow_urn, job_name)

            upstreams = []
            for up_name in sorted(job.upstream_job_names()):
                # Upstream jobs may be in another box; fall back to synthetic flow if unknown.
                up_obj = exp.jobs.get(up_name)
                up_flow = box_flow_urns.get(up_obj.box_name) if up_obj and up_obj.box_name else synthetic_flow_urn
                upstreams.append(datajob_urn(up_flow, up_name))

            if upstreams:
                mcps.append(
                    mcp_datajob_io(
                        job_urn=job_urn,
                        input_datasets=[],
                        output_datasets=[],
                        input_datajob_edges=[make_edge(u) for u in upstreams],
                    )
                )

        for wu in as_workunits("autosys", mcps):
            self.report.report_workunit()
            yield wu
