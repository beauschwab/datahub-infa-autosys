from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Iterator, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit


@dataclass
class SimpleReport:
    emitted: int = 0
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def report_workunit(self) -> None:
        self.emitted += 1

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)

    def error(self, msg: str) -> None:
        self.errors.append(msg)


def as_workunits(prefix: str, mcps: Iterable[MetadataChangeProposalWrapper]) -> Iterator[MetadataWorkUnit]:
    for i, mcp in enumerate(mcps):
        yield MetadataWorkUnit(id=f"{prefix}:{i}", mcp=mcp)
