"""
Pydantic models for Essbase metadata entities.

These models represent the multidimensional schema structure of Essbase
and map to OpenLineage/DataHub concepts.
"""

from __future__ import annotations
from datetime import datetime
from enum import Enum
from typing import Optional, Any
from pydantic import BaseModel, Field


class DimensionType(str, Enum):
    """Essbase dimension storage types."""
    DENSE = "dense"
    SPARSE = "sparse"


class DimensionCategory(str, Enum):
    """Essbase dimension categories."""
    ACCOUNTS = "accounts"
    TIME = "time"
    COUNTRY = "country"
    ATTRIBUTE = "attribute"
    STANDARD = "standard"


class MemberConsolidation(str, Enum):
    """Member consolidation operators."""
    ADDITION = "+"
    SUBTRACTION = "-"
    MULTIPLICATION = "*"
    DIVISION = "/"
    PERCENT = "%"
    IGNORE = "~"
    NEVER = "^"


class MemberStorage(str, Enum):
    """Member data storage types."""
    STORE_DATA = "store"
    DYNAMIC_CALC = "dynamic_calc"
    DYNAMIC_CALC_AND_STORE = "dynamic_calc_store"
    NEVER_SHARE = "never_share"
    LABEL_ONLY = "label_only"
    SHARED_MEMBER = "shared"


class CalcOperation(str, Enum):
    """Types of calculation operations for lineage."""
    AGGREGATION = "aggregation"
    ALLOCATION = "allocation"
    FORMULA = "formula"
    COPY = "copy"
    CLEAR = "clear"
    DATACOPY = "datacopy"
    FIX = "fix"
    CALC_DIM = "calc_dim"
    MEMBER_FORMULA = "member_formula"
    CUSTOM = "custom"


# =============================================================================
# Core Essbase Schema Models
# =============================================================================

class EssbaseMember(BaseModel):
    """Represents an Essbase dimension member."""
    name: str
    alias: Optional[str] = None
    parent: Optional[str] = None
    children: list[str] = Field(default_factory=list)
    level: int = 0
    generation: int = 0
    consolidation: MemberConsolidation = MemberConsolidation.ADDITION
    storage: MemberStorage = MemberStorage.STORE_DATA
    formula: Optional[str] = None
    uda: list[str] = Field(default_factory=list, description="User-defined attributes")
    attributes: dict[str, Any] = Field(default_factory=dict)
    
    # For accounts dimension
    time_balance: Optional[str] = None  # first, last, average
    variance_reporting: Optional[str] = None  # expense, non-expense
    
    # Metadata
    comment: Optional[str] = None
    
    def has_formula(self) -> bool:
        return self.formula is not None and len(self.formula.strip()) > 0
    
    def is_calculated(self) -> bool:
        return self.storage in [MemberStorage.DYNAMIC_CALC, MemberStorage.DYNAMIC_CALC_AND_STORE]


class EssbaseDimension(BaseModel):
    """Represents an Essbase dimension."""
    name: str
    alias_table: Optional[str] = None
    dimension_type: DimensionType = DimensionType.SPARSE
    category: DimensionCategory = DimensionCategory.STANDARD
    members: list[EssbaseMember] = Field(default_factory=list)
    member_count: int = 0
    level_count: int = 0
    generation_count: int = 0
    
    # Attribute dimensions linked to this dimension
    attribute_dimensions: list[str] = Field(default_factory=list)
    
    # For time dimensions
    time_periods: Optional[dict[str, Any]] = None
    
    def get_member(self, name: str) -> Optional[EssbaseMember]:
        for member in self.members:
            if member.name == name:
                return member
        return None
    
    def get_leaf_members(self) -> list[EssbaseMember]:
        return [m for m in self.members if not m.children]
    
    def get_calculated_members(self) -> list[EssbaseMember]:
        return [m for m in self.members if m.is_calculated() or m.has_formula()]


class EssbaseCube(BaseModel):
    """Represents an Essbase cube (database)."""
    name: str
    application: str
    description: Optional[str] = None
    dimensions: list[EssbaseDimension] = Field(default_factory=list)
    
    # Storage properties
    block_size: Optional[int] = None
    data_cache_size: Optional[int] = None
    index_cache_size: Optional[int] = None
    
    # Statistics
    number_of_blocks: Optional[int] = None
    block_density: Optional[float] = None
    compression_ratio: Optional[float] = None
    disk_volume: Optional[int] = None
    
    # Timestamps
    created_time: Optional[datetime] = None
    modified_time: Optional[datetime] = None
    last_restructure: Optional[datetime] = None
    
    def get_dense_dimensions(self) -> list[EssbaseDimension]:
        return [d for d in self.dimensions if d.dimension_type == DimensionType.DENSE]
    
    def get_sparse_dimensions(self) -> list[EssbaseDimension]:
        return [d for d in self.dimensions if d.dimension_type == DimensionType.SPARSE]


class EssbaseApplication(BaseModel):
    """Represents an Essbase application."""
    name: str
    description: Optional[str] = None
    cubes: list[EssbaseCube] = Field(default_factory=list)
    
    # Application settings
    app_type: str = "ASO"  # BSO, ASO, Hybrid
    unicode_enabled: bool = False
    
    # Security
    minimum_permission: Optional[str] = None
    
    # Statistics
    status: str = "running"
    created_time: Optional[datetime] = None
    modified_time: Optional[datetime] = None


# =============================================================================
# Calculation Script Models  
# =============================================================================

class CalcVariable(BaseModel):
    """A variable defined in a calc script."""
    name: str
    value: Optional[str] = None
    scope: str = "local"  # local, global


class CalcFixStatement(BaseModel):
    """A FIX statement defining calculation scope."""
    members: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Dimension name -> list of fixed members"
    )
    nested_operations: list[CalcTransform] = Field(default_factory=list)


class CalcTransform(BaseModel):
    """Represents a transformation operation in a calc script."""
    operation_type: CalcOperation
    target_members: list[str] = Field(default_factory=list)
    source_members: list[str] = Field(default_factory=list)
    formula: Optional[str] = None
    fix_context: Optional[dict[str, list[str]]] = None
    line_number: Optional[int] = None
    raw_statement: Optional[str] = None
    
    # For datacopy operations
    source_region: Optional[str] = None
    target_region: Optional[str] = None


class EssbaseCalcScript(BaseModel):
    """Represents an Essbase calculation script."""
    name: str
    cube: str
    application: str
    content: str
    
    # Parsed elements
    variables: list[CalcVariable] = Field(default_factory=list)
    fix_statements: list[CalcFixStatement] = Field(default_factory=list)
    transforms: list[CalcTransform] = Field(default_factory=list)
    
    # Referenced members
    referenced_members: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Dimension -> members referenced"
    )
    
    # Metadata
    description: Optional[str] = None
    created_time: Optional[datetime] = None
    modified_time: Optional[datetime] = None
    last_run_time: Optional[datetime] = None
    last_run_duration_ms: Optional[int] = None


# =============================================================================
# Load Rule Models
# =============================================================================

class LoadRuleFieldMapping(BaseModel):
    """Mapping of a source field to Essbase dimension/member."""
    field_number: int
    field_name: Optional[str] = None
    target_dimension: str
    target_member: Optional[str] = None  # For data load
    transformation: Optional[str] = None  # join, select, etc.
    start_position: Optional[int] = None
    length: Optional[int] = None


class LoadRuleDataMapping(BaseModel):
    """Mapping for data column in load rule."""
    field_number: int
    scale_factor: Optional[float] = None
    data_operation: str = "overwrite"  # overwrite, add, subtract


class EssbaseLoadRule(BaseModel):
    """Represents an Essbase load rule."""
    name: str
    cube: str
    application: str
    rule_type: str = "data"  # data, dimension
    
    # Source info
    source_type: str = "file"  # file, sql
    source_path: Optional[str] = None
    sql_query: Optional[str] = None
    sql_connection: Optional[str] = None
    
    # Field mappings
    field_mappings: list[LoadRuleFieldMapping] = Field(default_factory=list)
    data_mappings: list[LoadRuleDataMapping] = Field(default_factory=list)
    
    # Header/skip settings
    header_records: int = 0
    skip_records: int = 0
    
    # Delimiters
    field_delimiter: str = ","
    text_delimiter: Optional[str] = '"'
    
    # Metadata
    description: Optional[str] = None
    created_time: Optional[datetime] = None
    modified_time: Optional[datetime] = None


# =============================================================================
# Operational Statistics Models
# =============================================================================

class CubeStatistics(BaseModel):
    """Runtime statistics for an Essbase cube."""
    cube: str
    application: str
    timestamp: datetime = Field(default_factory=datetime.now)
    
    # Size metrics
    data_file_size_bytes: Optional[int] = None
    index_file_size_bytes: Optional[int] = None
    page_file_size_bytes: Optional[int] = None
    
    # Block metrics
    existing_blocks: Optional[int] = None
    potential_blocks: Optional[int] = None
    block_density_percent: Optional[float] = None
    average_cluster_ratio: Optional[float] = None
    average_fragmentation_quotient: Optional[float] = None
    
    # Cache utilization
    data_cache_hit_ratio: Optional[float] = None
    index_cache_hit_ratio: Optional[float] = None
    
    # Query statistics
    queries_executed: Optional[int] = None
    average_query_time_ms: Optional[float] = None


class SessionInfo(BaseModel):
    """Active session information."""
    session_id: str
    user_name: str
    cube: str
    application: str
    login_time: datetime
    last_active: Optional[datetime] = None
    client_type: Optional[str] = None
    client_ip: Optional[str] = None


class JobExecution(BaseModel):
    """Record of a job execution (calc, load, etc.)."""
    job_id: str
    job_type: str  # calc, dataload, dimload, export
    cube: str
    application: str
    
    # Execution details
    script_name: Optional[str] = None
    started_time: datetime
    ended_time: Optional[datetime] = None
    duration_ms: Optional[int] = None
    status: str = "running"  # running, completed, failed
    
    # Stats
    records_processed: Optional[int] = None
    records_rejected: Optional[int] = None
    error_message: Optional[str] = None
    user_name: Optional[str] = None
