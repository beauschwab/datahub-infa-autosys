"""
Minimal Essbase schema models used for extraction.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class DimensionType(str, Enum):
    DENSE = "dense"
    SPARSE = "sparse"


class DimensionCategory(str, Enum):
    ACCOUNTS = "accounts"
    TIME = "time"
    COUNTRY = "country"
    ATTRIBUTE = "attribute"
    STANDARD = "standard"


class MemberConsolidation(str, Enum):
    ADDITION = "+"
    SUBTRACTION = "-"
    MULTIPLICATION = "*"
    DIVISION = "/"
    PERCENT = "%"
    IGNORE = "~"
    NEVER = "^"


class MemberStorage(str, Enum):
    STORE_DATA = "store"
    DYNAMIC_CALC = "dynamic_calc"
    DYNAMIC_CALC_AND_STORE = "dynamic_calc_store"
    NEVER_SHARE = "never_share"
    LABEL_ONLY = "label_only"
    SHARED_MEMBER = "shared"


class EssbaseMember(BaseModel):
    name: str
    alias: Optional[str] = None
    parent: Optional[str] = None
    children: list[str] = Field(default_factory=list)
    level: int = 0
    generation: int = 0
    consolidation: MemberConsolidation = MemberConsolidation.ADDITION
    storage: MemberStorage = MemberStorage.STORE_DATA
    formula: Optional[str] = None
    uda: list[str] = Field(default_factory=list)
    time_balance: Optional[str] = None
    variance_reporting: Optional[str] = None
    comment: Optional[str] = None

    def has_formula(self) -> bool:
        return self.formula is not None and len(self.formula.strip()) > 0

    def is_calculated(self) -> bool:
        return self.storage in [MemberStorage.DYNAMIC_CALC, MemberStorage.DYNAMIC_CALC_AND_STORE]


class EssbaseDimension(BaseModel):
    name: str
    dimension_type: DimensionType = DimensionType.SPARSE
    category: DimensionCategory = DimensionCategory.STANDARD
    members: list[EssbaseMember] = Field(default_factory=list)
    member_count: int = 0
    level_count: int = 0
    generation_count: int = 0

    def get_member(self, name: str) -> Optional[EssbaseMember]:
        for member in self.members:
            if member.name == name:
                return member
        return None


class EssbaseCube(BaseModel):
    name: str
    application: str
    description: Optional[str] = None
    dimensions: list[EssbaseDimension] = Field(default_factory=list)
    number_of_blocks: Optional[int] = None
    block_density: Optional[float] = None
    disk_volume: Optional[int] = None
    created_time: Optional[datetime] = None
    modified_time: Optional[datetime] = None

    def get_dense_dimensions(self) -> list[EssbaseDimension]:
        return [d for d in self.dimensions if d.dimension_type == DimensionType.DENSE]

    def get_sparse_dimensions(self) -> list[EssbaseDimension]:
        return [d for d in self.dimensions if d.dimension_type == DimensionType.SPARSE]


class EssbaseApplication(BaseModel):
    name: str
    description: Optional[str] = None
    cubes: list[EssbaseCube] = Field(default_factory=list)
    status: str = "running"
    created_time: Optional[datetime] = None
    modified_time: Optional[datetime] = None