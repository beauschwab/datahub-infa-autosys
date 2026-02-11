from __future__ import annotations

from typing import Dict, Iterator

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.source import Source

from datahub_custom_sources.config import OracleOperationalConfig
from datahub_custom_sources.sources.common import SimpleReport


class OracleOperationalSource(Source):
    """
    NOTE: Operational lineage is *event-like* and often best emitted continuously
    (polling audit logs / v$sql / scheduler logs). The DataHub ingestion CLI is
    batch-oriented.

    This source is intentionally minimal and intended to be extended for your environment.
    We ship a reference implementation under `datahub_custom_sources/operational/oracle_runner.py`
    that uses the SDK emitter to stream DataProcessInstance events.
    """

    def __init__(self, ctx: PipelineContext, config: OracleOperationalConfig):
        super().__init__(ctx)
        self.ctx = ctx
        self.config = config
        self.report = SimpleReport()
        self.report.warn(
            "OracleOperationalSource is a stub. Use `dhcs` + datahub_custom_sources.operational.oracle_runner for streaming operational lineage."
        )

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "OracleOperationalSource":
        config = OracleOperationalConfig.model_validate(config_dict)
        return cls(ctx, config)

    def get_workunits(self) -> Iterator:
        # no-op
        if False:
            yield None  # pragma: no cover

    def get_report(self) -> SimpleReport:
        return self.report

    def close(self) -> None:
        pass
