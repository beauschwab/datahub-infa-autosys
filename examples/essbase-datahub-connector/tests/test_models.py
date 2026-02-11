"""Tests for Pydantic models and YAML serialization."""

import pytest
import tempfile
from pathlib import Path
from datetime import datetime

from essbase_datahub.models import (
    EssbaseMember,
    EssbaseDimension,
    EssbaseCube,
    EssbaseApplication,
    DimensionType,
    DimensionCategory,
    MemberStorage,
    MemberConsolidation,
    OpenLineageDataset,
    SchemaField,
    SchemaFieldType,
    EssbaseFieldMetadata,
    ColumnLineageMapping,
    OpenLineageJob,
    EssbaseMetadataExtraction,
)


class TestEssbaseModels:
    """Test Essbase Pydantic models."""
    
    def test_member_creation(self):
        """Test creating a member with all properties."""
        member = EssbaseMember(
            name="Sales",
            alias="Net Sales",
            parent="Measures",
            level=1,
            generation=2,
            consolidation=MemberConsolidation.ADDITION,
            storage=MemberStorage.STORE_DATA,
            formula=None,
            uda=["Revenue", "TopLine"],
        )
        
        assert member.name == "Sales"
        assert member.alias == "Net Sales"
        assert member.consolidation == MemberConsolidation.ADDITION
        assert not member.has_formula()
        assert not member.is_calculated()
    
    def test_calculated_member(self):
        """Test calculated member detection."""
        member = EssbaseMember(
            name="Profit",
            storage=MemberStorage.DYNAMIC_CALC,
            formula='"Sales" - "COGS"',
        )
        
        assert member.has_formula()
        assert member.is_calculated()
    
    def test_dimension_creation(self):
        """Test creating a dimension with members."""
        members = [
            EssbaseMember(name="Total", level=0),
            EssbaseMember(name="Q1", parent="Total", level=1),
            EssbaseMember(name="Jan", parent="Q1", level=2),
            EssbaseMember(name="Feb", parent="Q1", level=2),
            EssbaseMember(name="Mar", parent="Q1", level=2),
        ]
        
        dim = EssbaseDimension(
            name="Year",
            dimension_type=DimensionType.DENSE,
            category=DimensionCategory.TIME,
            members=members,
            member_count=5,
            level_count=3,
        )
        
        assert dim.name == "Year"
        assert dim.dimension_type == DimensionType.DENSE
        assert len(dim.members) == 5
        
        # Test member lookup
        jan = dim.get_member("Jan")
        assert jan is not None
        assert jan.parent == "Q1"
    
    def test_cube_dimension_accessors(self):
        """Test cube's dense/sparse dimension accessors."""
        cube = EssbaseCube(
            name="Plan",
            application="Finance",
            dimensions=[
                EssbaseDimension(name="Year", dimension_type=DimensionType.DENSE),
                EssbaseDimension(name="Measures", dimension_type=DimensionType.DENSE),
                EssbaseDimension(name="Market", dimension_type=DimensionType.SPARSE),
                EssbaseDimension(name="Product", dimension_type=DimensionType.SPARSE),
            ],
        )
        
        dense = cube.get_dense_dimensions()
        sparse = cube.get_sparse_dimensions()
        
        assert len(dense) == 2
        assert len(sparse) == 2
        assert all(d.dimension_type == DimensionType.DENSE for d in dense)


class TestOpenLineageModels:
    """Test OpenLineage-compatible models."""
    
    def test_schema_field_with_essbase_metadata(self):
        """Test schema field with Essbase-specific metadata."""
        essbase_meta = EssbaseFieldMetadata(
            dimension_name="Measures",
            member_name="Profit",
            level=0,
            generation=1,
            consolidation="~",
            storage_type="dynamic_calc",
            formula='"Sales" - "COGS"',
            is_calculated=True,
        )
        
        field = SchemaField(
            name="Profit",
            type=SchemaFieldType.MEASURE,
            description="Net profit calculation",
            tags=["calculated", "formula"],
            essbase_metadata=essbase_meta,
        )
        
        assert field.name == "Profit"
        assert field.essbase_metadata.formula == '"Sales" - "COGS"'
    
    def test_nested_schema_fields(self):
        """Test hierarchical schema fields."""
        jan = SchemaField(name="Jan", type=SchemaFieldType.MEMBER)
        feb = SchemaField(name="Feb", type=SchemaFieldType.MEMBER)
        mar = SchemaField(name="Mar", type=SchemaFieldType.MEMBER)
        
        q1 = SchemaField(
            name="Q1",
            type=SchemaFieldType.MEMBER,
            fields=[jan, feb, mar],
        )
        
        year = SchemaField(
            name="Year",
            type=SchemaFieldType.DIMENSION,
            fields=[q1],
        )
        
        assert len(year.fields) == 1
        assert len(year.fields[0].fields) == 3
    
    def test_column_lineage_mapping(self):
        """Test column-level lineage mapping."""
        mapping = ColumnLineageMapping(
            source_dataset="Finance.Plan",
            source_field="Measures.Sales",
            target_dataset="Finance.Plan",
            target_field="Measures.Profit",
            transformation_type="member_formula",
            transformation_expression='"Sales" - "COGS"',
            confidence=1.0,
        )
        
        assert mapping.source_field == "Measures.Sales"
        assert mapping.transformation_type == "member_formula"
    
    def test_dataset_urn_generation(self):
        """Test DataHub URN generation for datasets."""
        dataset = OpenLineageDataset(
            namespace="essbase.company.com",
            name="Finance.Plan",
            platform="essbase",
            platform_instance="prod",
        )
        
        urn = dataset.to_urn()
        
        assert "urn:li:dataset" in urn
        assert "essbase" in urn
        assert "Finance.Plan" in urn


class TestYAMLSerialization:
    """Test YAML serialization and deserialization."""
    
    def test_extraction_to_yaml(self):
        """Test serializing extraction to YAML."""
        extraction = EssbaseMetadataExtraction(
            essbase_server="essbase.company.com",
            essbase_version="21.4.0.0.0",
            datasets=[
                OpenLineageDataset(
                    namespace="essbase.company.com",
                    name="Sample.Basic",
                    platform="essbase",
                    tags=["essbase", "olap"],
                )
            ],
            jobs=[
                OpenLineageJob(
                    namespace="essbase.company.com",
                    name="Sample.Basic.CalcAll",
                    platform="essbase",
                    job_type="essbase_calculation",
                )
            ],
        )
        
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
            extraction.to_yaml(f.name)
            yaml_path = Path(f.name)
        
        try:
            # Verify file was created
            assert yaml_path.exists()
            
            # Verify it can be read back
            loaded = EssbaseMetadataExtraction.from_yaml(str(yaml_path))
            
            assert loaded.essbase_server == "essbase.company.com"
            assert len(loaded.datasets) == 1
            assert len(loaded.jobs) == 1
            assert loaded.datasets[0].name == "Sample.Basic"
        finally:
            yaml_path.unlink()
    
    def test_yaml_string_output(self):
        """Test YAML string serialization."""
        extraction = EssbaseMetadataExtraction(
            essbase_server="test.server.com",
            datasets=[
                OpenLineageDataset(
                    namespace="test.server.com",
                    name="App.Cube",
                    platform="essbase",
                )
            ],
        )
        
        yaml_str = extraction.to_yaml_string()
        
        assert "test.server.com" in yaml_str
        assert "App.Cube" in yaml_str
        assert "essbase" in yaml_str


class TestModelValidation:
    """Test Pydantic validation."""
    
    def test_member_consolidation_values(self):
        """Test member consolidation enum values."""
        for op in ["+", "-", "*", "/", "%", "~", "^"]:
            consol = MemberConsolidation(op)
            assert consol.value == op
    
    def test_dimension_type_values(self):
        """Test dimension type enum values."""
        assert DimensionType.DENSE.value == "dense"
        assert DimensionType.SPARSE.value == "sparse"
    
    def test_schema_field_types(self):
        """Test all schema field types."""
        for field_type in SchemaFieldType:
            field = SchemaField(
                name="test",
                type=field_type,
            )
            assert field.type == field_type
