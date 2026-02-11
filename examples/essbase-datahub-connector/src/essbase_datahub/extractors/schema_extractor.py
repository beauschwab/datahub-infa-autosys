"""
Schema extractor for Essbase cubes and dimensions.

Extracts multidimensional schema information and converts to
OpenLineage-compatible dataset representations.
"""

from __future__ import annotations
import logging
from typing import Optional, Any
from datetime import datetime

from .essbase_client import EssbaseClient
from ..models.essbase_models import (
    EssbaseApplication,
    EssbaseCube,
    EssbaseDimension,
    EssbaseMember,
    DimensionType,
    DimensionCategory,
    MemberConsolidation,
    MemberStorage,
    CubeStatistics,
)
from ..models.openlineage_models import (
    OpenLineageDataset,
    DatasetFacets,
    SchemaField,
    SchemaFieldType,
    EssbaseFieldMetadata,
)

logger = logging.getLogger(__name__)


class SchemaExtractor:
    """
    Extracts schema metadata from Essbase applications and cubes.
    
    Converts Essbase multidimensional structure to flat schema fields
    suitable for DataHub, while preserving hierarchy information in
    custom properties.
    """
    
    def __init__(
        self,
        client: EssbaseClient,
        include_members: bool = True,
        max_member_depth: int = -1,
        include_formulas: bool = True,
        include_aliases: bool = True,
        include_udas: bool = True,
    ):
        self.client = client
        self.include_members = include_members
        self.max_member_depth = max_member_depth
        self.include_formulas = include_formulas
        self.include_aliases = include_aliases
        self.include_udas = include_udas
    
    # =========================================================================
    # Application Extraction
    # =========================================================================
    
    def extract_application(self, app_name: str) -> EssbaseApplication:
        """Extract complete application metadata."""
        logger.info(f"Extracting application: {app_name}")
        
        app_data = self.client.get_application(app_name)
        if not app_data:
            raise ValueError(f"Application not found: {app_name}")
        
        app = EssbaseApplication(
            name=app_name,
            description=app_data.get("description"),
            app_type=app_data.get("type", "BSO"),
            status=app_data.get("status", "running"),
            created_time=self._parse_timestamp(app_data.get("createdTime")),
            modified_time=self._parse_timestamp(app_data.get("modifiedTime")),
        )
        
        # Extract all cubes
        cube_list = self.client.list_cubes(app_name)
        for cube_info in cube_list:
            cube = self.extract_cube(app_name, cube_info["name"])
            app.cubes.append(cube)
        
        return app
    
    # =========================================================================
    # Cube Extraction
    # =========================================================================
    
    def extract_cube(self, app_name: str, cube_name: str) -> EssbaseCube:
        """Extract complete cube metadata including dimensions."""
        logger.info(f"Extracting cube: {app_name}.{cube_name}")
        
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
        
        # Extract statistics
        try:
            stats = self.client.get_cube_statistics(app_name, cube_name)
            cube.number_of_blocks = stats.get("numberOfBlocks")
            cube.block_density = stats.get("blockDensity")
            cube.disk_volume = stats.get("diskVolume")
        except Exception as e:
            logger.warning(f"Could not get statistics for {app_name}.{cube_name}: {e}")
        
        # Extract dimensions
        dim_list = self.client.list_dimensions(app_name, cube_name)
        for dim_info in dim_list:
            dim = self.extract_dimension(app_name, cube_name, dim_info["name"])
            cube.dimensions.append(dim)
        
        return cube
    
    # =========================================================================
    # Dimension Extraction
    # =========================================================================
    
    def extract_dimension(
        self,
        app_name: str,
        cube_name: str,
        dim_name: str,
    ) -> EssbaseDimension:
        """Extract dimension with optional member hierarchy."""
        logger.info(f"Extracting dimension: {app_name}.{cube_name}.{dim_name}")
        
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
        
        # Extract members if configured
        if self.include_members:
            members = self._extract_members(app_name, cube_name, dim_name)
            dim.members = members
        
        return dim
    
    def _extract_members(
        self,
        app_name: str,
        cube_name: str,
        dim_name: str,
    ) -> list[EssbaseMember]:
        """Extract all members from a dimension."""
        members = []
        member_map: dict[str, EssbaseMember] = {}
        
        for member_data in self.client.iterate_all_members(app_name, cube_name, dim_name):
            member = self._parse_member(member_data)
            members.append(member)
            member_map[member.name] = member
        
        # Build parent-child relationships
        for member in members:
            if member.parent and member.parent in member_map:
                parent = member_map[member.parent]
                if member.name not in parent.children:
                    parent.children.append(member.name)
        
        # Get detailed info for members with formulas
        if self.include_formulas:
            for member in members:
                if member.has_formula() or member.is_calculated():
                    try:
                        details = self.client.get_member_details(
                            app_name, cube_name, member.name
                        )
                        if details and "formula" in details:
                            member.formula = details["formula"]
                    except Exception as e:
                        logger.warning(f"Could not get formula for {member.name}: {e}")
        
        return members
    
    def _parse_member(self, data: dict) -> EssbaseMember:
        """Parse member data from API response."""
        return EssbaseMember(
            name=data.get("name", ""),
            alias=data.get("alias") if self.include_aliases else None,
            parent=data.get("parentName"),
            level=data.get("levelNumber", 0),
            generation=data.get("generationNumber", 0),
            consolidation=self._map_consolidation(data.get("consolidation")),
            storage=self._map_storage(data.get("dataStorage")),
            formula=data.get("formula"),
            uda=data.get("udas", []) if self.include_udas else [],
            time_balance=data.get("timeBalance"),
            variance_reporting=data.get("varianceReporting"),
            comment=data.get("comment"),
        )
    
    # =========================================================================
    # Statistics Extraction
    # =========================================================================
    
    def extract_cube_statistics(self, app_name: str, cube_name: str) -> CubeStatistics:
        """Extract operational statistics for a cube."""
        stats_data = self.client.get_cube_statistics(app_name, cube_name)
        
        return CubeStatistics(
            cube=cube_name,
            application=app_name,
            timestamp=datetime.now(),
            data_file_size_bytes=stats_data.get("dataFileSize"),
            index_file_size_bytes=stats_data.get("indexFileSize"),
            page_file_size_bytes=stats_data.get("pageFileSize"),
            existing_blocks=stats_data.get("existingBlocks"),
            potential_blocks=stats_data.get("potentialBlocks"),
            block_density_percent=stats_data.get("blockDensity"),
            average_cluster_ratio=stats_data.get("averageClusterRatio"),
            average_fragmentation_quotient=stats_data.get("averageFragmentationQuotient"),
        )
    
    # =========================================================================
    # OpenLineage Dataset Conversion
    # =========================================================================
    
    def to_openlineage_dataset(
        self,
        cube: EssbaseCube,
        namespace: str,
        platform_instance: Optional[str] = None,
    ) -> OpenLineageDataset:
        """Convert Essbase cube to OpenLineage dataset."""
        dataset_name = f"{cube.application}.{cube.name}"
        
        # Build schema fields from dimensions
        schema_fields = []
        for dim in cube.dimensions:
            dim_field = self._dimension_to_schema_field(dim)
            schema_fields.append(dim_field)
        
        # Add a "Measures" virtual field representing data values
        measures_field = SchemaField(
            name="__measures__",
            type=SchemaFieldType.DOUBLE,
            description="Data values at dimension intersections",
            tags=["measures", "fact"],
        )
        schema_fields.append(measures_field)
        
        # Build facets
        facets = DatasetFacets(
            schema_facet={
                "_producer": "essbase-datahub-connector",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
                "fields": [f.model_dump(exclude_none=True) for f in schema_fields],
            },
            documentation={
                "_producer": "essbase-datahub-connector",
                "description": cube.description or f"Essbase cube: {dataset_name}",
            },
            essbase_cube_metadata={
                "cube_type": "BSO",  # TODO: detect from cube
                "dense_dimensions": [d.name for d in cube.get_dense_dimensions()],
                "sparse_dimensions": [d.name for d in cube.get_sparse_dimensions()],
                "number_of_blocks": cube.number_of_blocks,
                "block_density": cube.block_density,
            },
        )
        
        # Custom properties
        custom_props = {
            "essbase_application": cube.application,
            "essbase_cube": cube.name,
            "dimension_count": str(len(cube.dimensions)),
        }
        if cube.created_time:
            custom_props["created_time"] = cube.created_time.isoformat()
        if cube.modified_time:
            custom_props["modified_time"] = cube.modified_time.isoformat()
        
        return OpenLineageDataset(
            namespace=namespace,
            name=dataset_name,
            platform="essbase",
            platform_instance=platform_instance,
            facets=facets,
            schema_fields=schema_fields,
            custom_properties=custom_props,
            tags=["essbase", "multidimensional", "olap"],
        )
    
    def _dimension_to_schema_field(self, dim: EssbaseDimension) -> SchemaField:
        """Convert a dimension to a schema field with nested member fields."""
        # Determine field type based on dimension category
        field_type = SchemaFieldType.DIMENSION
        if dim.category == DimensionCategory.ACCOUNTS:
            field_type = SchemaFieldType.MEASURE
        elif dim.category == DimensionCategory.TIME:
            field_type = SchemaFieldType.DATE
        
        # Build nested fields for top-level members
        nested_fields = []
        root_members = [m for m in dim.members if m.parent is None or m.level == 0]
        
        for member in root_members[:100]:  # Limit for large dimensions
            member_field = self._member_to_schema_field(member, dim)
            nested_fields.append(member_field)
        
        # Build Essbase metadata
        essbase_meta = EssbaseFieldMetadata(
            dimension_name=dim.name,
            is_calculated=any(m.is_calculated() for m in dim.members),
        )
        
        # Build tags
        tags = [dim.dimension_type.value, dim.category.value]
        
        return SchemaField(
            name=dim.name,
            type=field_type,
            description=f"{dim.category.value.title()} dimension with {dim.member_count} members",
            fields=nested_fields,
            native_type=f"essbase_{dim.dimension_type.value}_dimension",
            tags=tags,
            custom_properties={
                "storage_type": dim.dimension_type.value,
                "category": dim.category.value,
                "member_count": str(dim.member_count),
                "level_count": str(dim.level_count),
                "generation_count": str(dim.generation_count),
            },
            essbase_metadata=essbase_meta,
        )
    
    def _member_to_schema_field(
        self,
        member: EssbaseMember,
        dimension: EssbaseDimension,
    ) -> SchemaField:
        """Convert a member to a nested schema field."""
        tags = []
        if member.is_calculated():
            tags.append("calculated")
        if member.has_formula():
            tags.append("formula")
        if member.storage == MemberStorage.SHARED_MEMBER:
            tags.append("shared")
        
        essbase_meta = EssbaseFieldMetadata(
            dimension_name=dimension.name,
            member_name=member.name,
            level=member.level,
            generation=member.generation,
            consolidation=member.consolidation.value,
            storage_type=member.storage.value,
            formula=member.formula,
            alias=member.alias,
            uda=member.uda,
            time_balance=member.time_balance,
            variance_reporting=member.variance_reporting,
            is_calculated=member.is_calculated(),
        )
        
        return SchemaField(
            name=member.name,
            type=SchemaFieldType.MEMBER,
            description=member.comment or member.alias,
            native_type=f"essbase_member_{member.storage.value}",
            tags=tags,
            custom_properties={
                "consolidation": member.consolidation.value,
                "storage": member.storage.value,
                "level": str(member.level),
                "generation": str(member.generation),
            },
            essbase_metadata=essbase_meta,
        )
    
    # =========================================================================
    # Helper Methods
    # =========================================================================
    
    def _map_dimension_type(self, storage_type: Optional[str]) -> DimensionType:
        """Map API storage type to enum."""
        if storage_type and storage_type.upper() == "DENSE":
            return DimensionType.DENSE
        return DimensionType.SPARSE
    
    def _map_dimension_category(self, dim_type: Optional[str]) -> DimensionCategory:
        """Map API dimension type to category enum."""
        mapping = {
            "ACCOUNTS": DimensionCategory.ACCOUNTS,
            "TIME": DimensionCategory.TIME,
            "COUNTRY": DimensionCategory.COUNTRY,
            "ATTRIBUTE": DimensionCategory.ATTRIBUTE,
        }
        return mapping.get(dim_type.upper() if dim_type else "", DimensionCategory.STANDARD)
    
    def _map_consolidation(self, value: Optional[str]) -> MemberConsolidation:
        """Map API consolidation value to enum."""
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
        """Map API storage value to enum."""
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
        """Parse ISO timestamp from API."""
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
