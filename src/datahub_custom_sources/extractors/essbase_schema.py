"""
Schema extraction and conversion utilities for Essbase cubes.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from datahub.metadata.schema_classes import (
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    NumberTypeClass,
    DateTypeClass,
)

from .essbase_client import EssbaseClient
from .essbase_models import (
    DimensionCategory,
    DimensionType,
    EssbaseCube,
    EssbaseDimension,
    EssbaseMember,
    MemberConsolidation,
    MemberStorage,
)

logger = logging.getLogger(__name__)


class SchemaExtractor:
    def __init__(
        self,
        client: EssbaseClient,
        include_members: bool = True,
        max_member_depth: int = -1,
        include_formulas: bool = True,
        include_aliases: bool = True,
        include_udas: bool = True,
        max_members_per_dimension: int = 200,
    ):
        self.client = client
        self.include_members = include_members
        self.max_member_depth = max_member_depth
        self.include_formulas = include_formulas
        self.include_aliases = include_aliases
        self.include_udas = include_udas
        self.max_members_per_dimension = max_members_per_dimension

    def extract_cube(self, app_name: str, cube_name: str) -> EssbaseCube:
        logger.info("Extracting cube: %s.%s", app_name, cube_name)

        cube_data = self.client.get_cube(app_name, cube_name)
        if not cube_data:
            raise ValueError(f"Cube not found: {app_name}.{cube_name}")

        cube = EssbaseCube(
            name=cube_name,
            application=app_name,
            description=cube_data.get("description"),
            created_time=self._parse_timestamp(cube_data.get("createdTime")),
            modified_time=self._parse_timestamp(cube_data.get("modifiedTime")),
        )

        try:
            stats = self.client.get_cube_statistics(app_name, cube_name)
            cube.number_of_blocks = stats.get("numberOfBlocks")
            cube.block_density = stats.get("blockDensity")
            cube.disk_volume = stats.get("diskVolume")
        except Exception as exc:
            logger.warning("Could not get statistics for %s.%s: %s", app_name, cube_name, exc)

        dim_list = self.client.list_dimensions(app_name, cube_name)
        for dim_info in dim_list:
            dim = self.extract_dimension(app_name, cube_name, dim_info["name"])
            cube.dimensions.append(dim)

        return cube

    def extract_dimension(self, app_name: str, cube_name: str, dim_name: str) -> EssbaseDimension:
        logger.info("Extracting dimension: %s.%s.%s", app_name, cube_name, dim_name)

        dim_data = self.client.get_dimension(app_name, cube_name, dim_name)
        if not dim_data:
            raise ValueError(f"Dimension not found: {dim_name}")

        dim = EssbaseDimension(
            name=dim_name,
            dimension_type=self._map_dimension_type(dim_data.get("storageType")),
            category=self._map_dimension_category(dim_data.get("dimensionType")),
            member_count=dim_data.get("numberOfMembers", 0),
            level_count=dim_data.get("numberOfLevels", 0),
            generation_count=dim_data.get("numberOfGenerations", 0),
        )

        if self.include_members:
            dim.members = self._extract_members(app_name, cube_name, dim_name)

        return dim

    def _extract_members(self, app_name: str, cube_name: str, dim_name: str) -> list[EssbaseMember]:
        members: list[EssbaseMember] = []
        member_map: dict[str, EssbaseMember] = {}

        for member_data in self.client.iterate_all_members(app_name, cube_name, dim_name):
            member = self._parse_member(member_data)
            members.append(member)
            member_map[member.name] = member
            if len(members) >= self.max_members_per_dimension:
                break

        for member in members:
            if member.parent and member.parent in member_map:
                parent = member_map[member.parent]
                if member.name not in parent.children:
                    parent.children.append(member.name)

        if self.include_formulas:
            for member in members:
                if member.has_formula() or member.is_calculated():
                    try:
                        details = self.client.get_member_details(app_name, cube_name, member.name)
                        if details and "formula" in details:
                            member.formula = details["formula"]
                    except Exception as exc:
                        logger.warning("Could not get formula for %s: %s", member.name, exc)

        return members

    def _parse_member(self, data: dict) -> EssbaseMember:
        return EssbaseMember(
            name=data.get("name", ""),
            alias=data.get("alias") if self.include_aliases else None,
            parent=data.get("parentName"),
            level=data.get("levelNumber", 0),
            generation=data.get("generationNumber", 0),
            consolidation=self._map_consolidation(data.get("consolidation")),
            storage=self._map_storage(data.get("dataStorage")),
            formula=data.get("formula") if self.include_formulas else None,
            uda=data.get("udas", []) if self.include_udas else [],
            time_balance=data.get("timeBalance"),
            variance_reporting=data.get("varianceReporting"),
            comment=data.get("comment"),
        )

    def build_schema_fields(self, cube: EssbaseCube) -> list[SchemaFieldClass]:
        fields: list[SchemaFieldClass] = []

        for dim in cube.dimensions:
            field_type = StringTypeClass()
            if dim.category == DimensionCategory.ACCOUNTS:
                field_type = NumberTypeClass()
            elif dim.category == DimensionCategory.TIME:
                field_type = DateTypeClass()

            fields.append(
                SchemaFieldClass(
                    fieldPath=dim.name,
                    type=SchemaFieldDataTypeClass(type=field_type),
                    nativeDataType=f"essbase_{dim.dimension_type.value}",
                    description=f"{dim.category.value} dimension",
                )
            )

            for member in dim.members[: self.max_members_per_dimension]:
                fields.append(
                    SchemaFieldClass(
                        fieldPath=f"{dim.name}.{member.name}",
                        type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                        nativeDataType=f"essbase_member_{member.storage.value}",
                        description=member.comment or member.alias,
                    )
                )

        return fields

    def _map_dimension_type(self, storage_type: Optional[str]) -> DimensionType:
        if storage_type and storage_type.upper() == "DENSE":
            return DimensionType.DENSE
        return DimensionType.SPARSE

    def _map_dimension_category(self, dim_type: Optional[str]) -> DimensionCategory:
        mapping = {
            "ACCOUNTS": DimensionCategory.ACCOUNTS,
            "TIME": DimensionCategory.TIME,
            "COUNTRY": DimensionCategory.COUNTRY,
            "ATTRIBUTE": DimensionCategory.ATTRIBUTE,
        }
        return mapping.get(dim_type.upper() if dim_type else "", DimensionCategory.STANDARD)

    def _map_consolidation(self, value: Optional[str]) -> MemberConsolidation:
        mapping = {
            "+": MemberConsolidation.ADDITION,
            "-": MemberConsolidation.SUBTRACTION,
            "*": MemberConsolidation.MULTIPLICATION,
            "/": MemberConsolidation.DIVISION,
            "%": MemberConsolidation.PERCENT,
            "~": MemberConsolidation.IGNORE,
            "^": MemberConsolidation.NEVER,
        }
        return mapping.get(value, MemberConsolidation.ADDITION)

    def _map_storage(self, value: Optional[str]) -> MemberStorage:
        if not value:
            return MemberStorage.STORE_DATA
        mapping = {
            "STORE": MemberStorage.STORE_DATA,
            "DYNAMIC_CALC": MemberStorage.DYNAMIC_CALC,
            "DYNAMIC_CALC_AND_STORE": MemberStorage.DYNAMIC_CALC_AND_STORE,
            "NEVER_SHARE": MemberStorage.NEVER_SHARE,
            "LABEL_ONLY": MemberStorage.LABEL_ONLY,
            "SHARED": MemberStorage.SHARED_MEMBER,
        }
        return mapping.get(value.upper(), MemberStorage.STORE_DATA)

    def _parse_timestamp(self, value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None