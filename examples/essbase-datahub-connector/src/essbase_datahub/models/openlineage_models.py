"""
OpenLineage-compatible models for YAML intermediate representation.

These models bridge Essbase metadata to DataHub's native format while
maintaining compatibility with OpenLineage spec for interoperability.
"""

from __future__ import annotations
from datetime import datetime
from enum import Enum
from typing import Optional, Any, Union
from pydantic import BaseModel, Field
import yaml


class OpenLineageRunState(str, Enum):
    """OpenLineage run states."""
    START = "START"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    ABORT = "ABORT"
    FAIL = "FAIL"


class SchemaFieldType(str, Enum):
    """Data types for schema fields."""
    STRING = "string"
    NUMBER = "number"
    INTEGER = "integer"
    DOUBLE = "double"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    ARRAY = "array"
    STRUCT = "struct"
    # Essbase-specific
    MEMBER = "member"
    DIMENSION = "dimension"
    MEASURE = "measure"


# =============================================================================
# Schema/Field Models (OpenLineage Dataset Schema compatible)
# =============================================================================

class SchemaField(BaseModel):
    """
    OpenLineage-compatible schema field.
    Maps to DataHub SchemaField.
    """
    name: str
    type: SchemaFieldType = SchemaFieldType.STRING
    description: Optional[str] = None
    
    # OpenLineage extensions
    fields: list[SchemaField] = Field(
        default_factory=list,
        description="Nested fields for hierarchical members"
    )
    
    # Custom properties for Essbase specifics
    native_type: Optional[str] = None
    custom_properties: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    glossary_terms: list[str] = Field(default_factory=list)
    
    # Essbase-specific metadata (stored in custom_properties for emission)
    essbase_metadata: Optional[EssbaseFieldMetadata] = None


class EssbaseFieldMetadata(BaseModel):
    """Essbase-specific field metadata."""
    dimension_name: Optional[str] = None
    member_name: Optional[str] = None
    level: Optional[int] = None
    generation: Optional[int] = None
    consolidation: Optional[str] = None
    storage_type: Optional[str] = None
    formula: Optional[str] = None
    alias: Optional[str] = None
    uda: list[str] = Field(default_factory=list)
    time_balance: Optional[str] = None
    variance_reporting: Optional[str] = None
    is_calculated: bool = False


# =============================================================================
# Dataset Models (OpenLineage Dataset compatible)
# =============================================================================

class DatasetFacets(BaseModel):
    """OpenLineage dataset facets."""
    schema_facet: Optional[dict[str, Any]] = Field(
        default=None,
        alias="schema"
    )
    datasource: Optional[dict[str, Any]] = None
    documentation: Optional[dict[str, Any]] = None
    ownership: Optional[dict[str, Any]] = None
    data_quality_metrics: Optional[dict[str, Any]] = None
    
    # Custom facets for Essbase
    essbase_cube_metadata: Optional[dict[str, Any]] = None
    essbase_statistics: Optional[dict[str, Any]] = None
    
    class Config:
        populate_by_name = True


class OpenLineageDataset(BaseModel):
    """
    OpenLineage-compatible dataset representation.
    
    For Essbase, a dataset typically represents:
    - A cube (the primary dataset)
    - A dimension (sub-dataset with hierarchical schema)
    - A data region defined by member intersections
    """
    namespace: str = Field(description="Essbase server/cluster identifier")
    name: str = Field(description="Fully qualified dataset name: app.cube[.dimension]")
    facets: DatasetFacets = Field(default_factory=DatasetFacets)
    
    # DataHub-specific extensions
    platform: str = "essbase"
    platform_instance: Optional[str] = None
    
    # Schema
    schema_fields: list[SchemaField] = Field(default_factory=list)
    
    # Custom properties
    custom_properties: dict[str, str] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    glossary_terms: list[str] = Field(default_factory=list)
    
    def to_urn(self) -> str:
        """Generate DataHub URN for this dataset."""
        instance = f".{self.platform_instance}" if self.platform_instance else ""
        return f"urn:li:dataset:(urn:li:dataPlatform:{self.platform}{instance},{self.name},PROD)"


# =============================================================================
# Column-Level Lineage Models
# =============================================================================

class ColumnLineageMapping(BaseModel):
    """
    Fine-grained column-level lineage mapping.
    Maps source columns to target columns with transformation info.
    """
    source_dataset: str
    source_field: str
    target_dataset: str
    target_field: str
    
    # Transformation details
    transformation_type: Optional[str] = None
    transformation_expression: Optional[str] = None
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    
    # Context
    aggregation_function: Optional[str] = None
    filter_expression: Optional[str] = None


class TransformOperation(BaseModel):
    """
    Detailed transformation operation for lineage.
    Captures the specifics of how data is transformed.
    """
    operation_id: str
    operation_type: str  # aggregation, formula, copy, allocation, etc.
    name: Optional[str] = None
    description: Optional[str] = None
    
    # Input/Output
    input_fields: list[ColumnLineageMapping] = Field(default_factory=list)
    output_fields: list[str] = Field(default_factory=list)
    
    # Logic
    logic_expression: Optional[str] = None
    parameters: dict[str, Any] = Field(default_factory=dict)
    
    # Execution context
    fix_context: Optional[dict[str, list[str]]] = None
    line_range: Optional[tuple[int, int]] = None


# =============================================================================
# Job/Run Models (OpenLineage Job & Run compatible)
# =============================================================================

class InputDataset(BaseModel):
    """Input dataset reference for a job."""
    namespace: str
    name: str
    input_facets: dict[str, Any] = Field(default_factory=dict)


class OutputDataset(BaseModel):
    """Output dataset reference for a job."""
    namespace: str
    name: str
    output_facets: dict[str, Any] = Field(default_factory=dict)


class OpenLineageJob(BaseModel):
    """
    OpenLineage-compatible job representation.
    
    For Essbase, jobs include:
    - Calculation scripts
    - Load rules (data and dimension)
    - Export operations
    """
    namespace: str
    name: str
    facets: dict[str, Any] = Field(default_factory=dict)
    
    # DataHub-specific
    platform: str = "essbase"
    flow_urn: Optional[str] = None
    
    # Job details
    job_type: str = "essbase_calculation"
    description: Optional[str] = None
    
    # Lineage
    inputs: list[InputDataset] = Field(default_factory=list)
    outputs: list[OutputDataset] = Field(default_factory=list)
    
    # Fine-grained lineage
    column_lineage: list[ColumnLineageMapping] = Field(default_factory=list)
    transform_operations: list[TransformOperation] = Field(default_factory=list)
    
    # Source code/logic
    source_code: Optional[str] = None
    source_url: Optional[str] = None
    
    # Custom properties
    custom_properties: dict[str, str] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    
    def to_urn(self) -> str:
        """Generate DataHub URN for this data job."""
        return f"urn:li:dataJob:(urn:li:dataFlow:({self.platform},{self.flow_urn or self.namespace},PROD),{self.name})"


class OpenLineageRunEvent(BaseModel):
    """
    OpenLineage run event for job executions.
    Maps to DataHub DataProcessInstance.
    """
    event_type: OpenLineageRunState
    event_time: datetime = Field(default_factory=datetime.utcnow)
    
    job: OpenLineageJob
    run_id: str
    
    # Run facets
    run_facets: dict[str, Any] = Field(default_factory=dict)
    
    # Inputs/Outputs (may differ from job definition)
    inputs: list[InputDataset] = Field(default_factory=list)
    outputs: list[OutputDataset] = Field(default_factory=list)


# =============================================================================
# Complete Extraction Result
# =============================================================================

class EssbaseMetadataExtraction(BaseModel):
    """
    Complete metadata extraction result ready for YAML serialization
    and DataHub emission.
    """
    # Extraction metadata
    extraction_timestamp: datetime = Field(default_factory=datetime.utcnow)
    essbase_server: str
    essbase_version: Optional[str] = None
    extractor_version: str = "0.1.0"
    
    # Datasets (cubes, dimensions as datasets)
    datasets: list[OpenLineageDataset] = Field(default_factory=list)
    
    # Jobs (calc scripts, load rules)
    jobs: list[OpenLineageJob] = Field(default_factory=list)
    
    # Run events (execution history)
    run_events: list[OpenLineageRunEvent] = Field(default_factory=list)
    
    # Global lineage (cross-cube, external sources)
    lineage_edges: list[ColumnLineageMapping] = Field(default_factory=list)
    
    # Operational metadata
    statistics: dict[str, Any] = Field(default_factory=dict)
    
    def to_yaml(self, path: str) -> None:
        """Serialize extraction to YAML file."""
        with open(path, 'w') as f:
            yaml.dump(
                self.model_dump(mode='json', exclude_none=True),
                f,
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True
            )
    
    @classmethod
    def from_yaml(cls, path: str) -> EssbaseMetadataExtraction:
        """Load extraction from YAML file."""
        with open(path, 'r') as f:
            data = yaml.safe_load(f)
        return cls.model_validate(data)
    
    def to_yaml_string(self) -> str:
        """Serialize to YAML string."""
        return yaml.dump(
            self.model_dump(mode='json', exclude_none=True),
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True
        )


# Update forward references
SchemaField.model_rebuild()
