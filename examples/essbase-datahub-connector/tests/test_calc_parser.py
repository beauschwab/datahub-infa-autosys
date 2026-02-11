"""Tests for the calculation script parser."""

import pytest
from essbase_datahub.extractors.calc_extractor import CalcScriptParser
from essbase_datahub.models.essbase_models import CalcOperation


class TestCalcScriptParser:
    """Test calc script parsing functionality."""
    
    @pytest.fixture
    def parser(self):
        return CalcScriptParser()
    
    def test_parse_simple_formula(self, parser):
        """Test parsing a simple member formula."""
        script = '''
        FIX("Actual")
            "Profit" = "Sales" - "COGS";
        ENDFIX
        '''
        
        result = parser.parse(script, "TestCalc", "Sample", "Basic")
        
        assert result.name == "TestCalc"
        assert result.cube == "Basic"
        assert result.application == "Sample"
        assert len(result.transforms) >= 1
        
        # Find the formula transform
        formula_transforms = [t for t in result.transforms if t.operation_type == CalcOperation.FORMULA]
        assert len(formula_transforms) == 1
        assert "Profit" in formula_transforms[0].target_members
    
    def test_parse_agg_operation(self, parser):
        """Test parsing AGG operation."""
        script = '''
        FIX("Actual")
            AGG("Year", "Market");
        ENDFIX
        '''
        
        result = parser.parse(script, "AggCalc", "Sample", "Basic")
        
        agg_transforms = [t for t in result.transforms if t.operation_type == CalcOperation.AGGREGATION]
        assert len(agg_transforms) == 1
        assert "Year" in agg_transforms[0].target_members
        assert "Market" in agg_transforms[0].target_members
    
    def test_parse_calc_dim(self, parser):
        """Test parsing CALC DIM operation."""
        script = '''
        CALC DIM("Measures", "Year");
        '''
        
        result = parser.parse(script, "CalcDimTest", "Sample", "Basic")
        
        calc_transforms = [t for t in result.transforms if t.operation_type == CalcOperation.CALC_DIM]
        assert len(calc_transforms) == 1
        assert "Measures" in calc_transforms[0].target_members
    
    def test_parse_datacopy(self, parser):
        """Test parsing DATACOPY operation."""
        script = '''
        FIX("Actual")
            DATACOPY "Budget" TO "Forecast";
        ENDFIX
        '''
        
        result = parser.parse(script, "DataCopyCalc", "Sample", "Basic")
        
        datacopy_transforms = [t for t in result.transforms if t.operation_type == CalcOperation.DATACOPY]
        assert len(datacopy_transforms) == 1
        assert "Budget" in datacopy_transforms[0].source_region
        assert "Forecast" in datacopy_transforms[0].target_region
    
    def test_parse_set_variables(self, parser):
        """Test parsing SET variables."""
        script = '''
        SET UPDATECALC OFF;
        SET AGGMISSG ON;
        VAR v_rate = 1.5;
        
        FIX("Actual")
            "Result" = "Input" * &v_rate;
        ENDFIX
        '''
        
        result = parser.parse(script, "VarCalc", "Sample", "Basic")
        
        assert len(result.variables) >= 2
        
        # Check SET variables
        set_vars = [v for v in result.variables if v.scope == "global"]
        assert len(set_vars) == 2
        
        # Check VAR declarations
        local_vars = [v for v in result.variables if v.scope == "local"]
        assert len(local_vars) >= 1
        assert any(v.name == "v_rate" for v in local_vars)
    
    def test_parse_nested_fix(self, parser):
        """Test parsing nested FIX statements."""
        script = '''
        FIX("Actual")
            FIX("East")
                "Sales" = "Sales" * 1.1;
            ENDFIX
            FIX("West")
                "Sales" = "Sales" * 1.2;
            ENDFIX
        ENDFIX
        '''
        
        result = parser.parse(script, "NestedFix", "Sample", "Basic")
        
        # Should have transforms with nested fix context
        formula_transforms = [t for t in result.transforms if t.operation_type == CalcOperation.FORMULA]
        assert len(formula_transforms) == 2
        
        # Both should have fix context
        for t in formula_transforms:
            assert t.fix_context is not None
    
    def test_parse_cleardata(self, parser):
        """Test parsing CLEARDATA operation."""
        script = '''
        CLEARDATA "Forecast";
        '''
        
        result = parser.parse(script, "ClearCalc", "Sample", "Basic")
        
        clear_transforms = [t for t in result.transforms if t.operation_type == CalcOperation.CLEAR]
        assert len(clear_transforms) == 1
        assert "Forecast" in clear_transforms[0].target_members
    
    def test_extract_member_references(self, parser):
        """Test extracting member references."""
        script = '''
        FIX(Year->"Jan", Market->"East")
            "Sales" = "Quantity" * "Price";
        ENDFIX
        '''
        
        result = parser.parse(script, "RefCalc", "Sample", "Basic")
        
        # Should have extracted member references
        assert "Year" in result.referenced_members or "_uncategorized_" in result.referenced_members


class TestCalcScriptFormulaParsing:
    """Test formula-specific parsing."""
    
    @pytest.fixture
    def parser(self):
        return CalcScriptParser()
    
    def test_formula_with_at_functions(self, parser):
        """Test parsing formulas with @ functions."""
        script = '''
        "Variance" = @VAR("Actual", "Budget");
        "YTD" = @SUM(@RANGE("Jan":"Dec"));
        '''
        
        result = parser.parse(script, "AtFuncCalc", "Sample", "Basic")
        
        formula_transforms = [t for t in result.transforms if t.operation_type == CalcOperation.FORMULA]
        assert len(formula_transforms) >= 2
    
    def test_formula_with_cross_dim(self, parser):
        """Test parsing cross-dimensional formulas."""
        script = '''
        "Variance" = "Actual" - "Budget"->"Scenario";
        "Prior Year" = "Actual"->(@PRIOR(Year));
        '''
        
        result = parser.parse(script, "CrossDimCalc", "Sample", "Basic")
        
        formula_transforms = [t for t in result.transforms if t.operation_type == CalcOperation.FORMULA]
        assert len(formula_transforms) >= 1
