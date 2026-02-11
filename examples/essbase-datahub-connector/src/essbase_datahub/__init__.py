"""
Essbase to DataHub Metadata Connector

Extracts multidimensional schema, calculation logic, and lineage from Oracle Essbase
and emits to DataHub using the Python SDK with OpenLineage-compatible YAML intermediate format.
"""

__version__ = "0.1.0"

from .extractors.essbase_client import EssbaseClient
from .extractors.schema_extractor import SchemaExtractor
from .extractors.calc_extractor import CalcScriptExtractor
from .extractors.lineage_extractor import LineageExtractor
from .emitters.datahub_emitter import DataHubEmitter
from .models.essbase_models import (
    EssbaseApplication,
    EssbaseCube,
    EssbaseDimension,
    EssbaseMember,
    EssbaseCalcScript,
    EssbaseLoadRule,
)

__all__ = [
    "EssbaseClient",
    "SchemaExtractor", 
    "CalcScriptExtractor",
    "LineageExtractor",
    "DataHubEmitter",
    "EssbaseApplication",
    "EssbaseCube",
    "EssbaseDimension",
    "EssbaseMember",
    "EssbaseCalcScript",
    "EssbaseLoadRule",
]
