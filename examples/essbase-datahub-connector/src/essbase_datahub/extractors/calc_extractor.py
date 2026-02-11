"""
Calculation script extractor and parser.

Extracts calc scripts from Essbase and parses them to identify:
- FIX statements (calculation scope)
- Member formulas
- CALC DIM operations
- DATACOPY operations
- Custom calculation logic
"""

from __future__ import annotations
import re
import logging
from typing import Optional, Any
from datetime import datetime

from .essbase_client import EssbaseClient
from ..models.essbase_models import (
    EssbaseCalcScript,
    CalcVariable,
    CalcFixStatement,
    CalcTransform,
    CalcOperation,
)

logger = logging.getLogger(__name__)


class CalcScriptParser:
    """
    Parser for Essbase calculation scripts.
    
    Extracts structural information including:
    - Variable definitions (SET, VAR)
    - FIX statements with member lists
    - Calculation operations (AGG, CALC DIM, etc.)
    - Member formula assignments
    - DATACOPY operations
    """
    
    # Regex patterns for calc script elements
    PATTERNS = {
        # SET variable = value;
        "set_var": re.compile(
            r'SET\s+(\w+)\s*=\s*([^;]+);',
            re.IGNORECASE
        ),
        # VAR variable = value;
        "var_def": re.compile(
            r'VAR\s+(\w+)\s*(?:=\s*([^;]+))?;',
            re.IGNORECASE
        ),
        # FIX (members...)
        "fix_start": re.compile(
            r'FIX\s*\(([^)]+)\)',
            re.IGNORECASE
        ),
        # ENDFIX
        "fix_end": re.compile(
            r'ENDFIX\s*;?',
            re.IGNORECASE
        ),
        # AGG (dimensions);
        "agg": re.compile(
            r'AGG\s*\(([^)]+)\)\s*;',
            re.IGNORECASE
        ),
        # CALC DIM (dimensions);
        "calc_dim": re.compile(
            r'CALC\s+DIM\s*\(([^)]+)\)\s*;',
            re.IGNORECASE
        ),
        # CALC ALL;
        "calc_all": re.compile(
            r'CALC\s+ALL\s*;',
            re.IGNORECASE
        ),
        # DATACOPY source TO target;
        "datacopy": re.compile(
            r'DATACOPY\s+([^T]+)\s+TO\s+([^;]+);',
            re.IGNORECASE
        ),
        # CLEARDATA member;
        "cleardata": re.compile(
            r'CLEARDATA\s+([^;]+);',
            re.IGNORECASE
        ),
        # CLEARBLOCK ALL/UPPER/NONINPUT;
        "clearblock": re.compile(
            r'CLEARBLOCK\s+(ALL|UPPER|NONINPUT)\s*;',
            re.IGNORECASE
        ),
        # Member = formula;
        "member_formula": re.compile(
            r'^[\s]*"?([^"=\n]+)"?\s*=\s*([^;]+);',
            re.MULTILINE
        ),
        # @function patterns
        "at_function": re.compile(
            r'@(\w+)\s*\([^)]*\)',
            re.IGNORECASE
        ),
        # Member references in quotes or brackets
        "member_ref": re.compile(
            r'"([^"]+)"|\'([^\']+)\'|\[([^\]]+)\]'
        ),
        # Dimension->member notation
        "dim_member": re.compile(
            r'(\w+)->(["\']?)([^"\'\s,;]+)\2'
        ),
        # Cross-dimensional operator
        "cross_dim": re.compile(
            r'->\s*\(([^)]+)\)'
        ),
    }
    
    def __init__(self):
        self.current_fix_context: list[dict[str, list[str]]] = []
    
    def parse(self, script_content: str, script_name: str, app: str, cube: str) -> EssbaseCalcScript:
        """Parse a complete calculation script."""
        logger.debug(f"Parsing calc script: {script_name}")
        
        script = EssbaseCalcScript(
            name=script_name,
            cube=cube,
            application=app,
            content=script_content,
        )
        
        # Extract variables
        script.variables = self._extract_variables(script_content)
        
        # Parse nested structure
        transforms, referenced_members = self._parse_script_body(script_content)
        script.transforms = transforms
        script.referenced_members = referenced_members
        
        return script
    
    def _extract_variables(self, content: str) -> list[CalcVariable]:
        """Extract SET and VAR declarations."""
        variables = []
        
        # SET variables
        for match in self.PATTERNS["set_var"].finditer(content):
            variables.append(CalcVariable(
                name=match.group(1),
                value=match.group(2).strip(),
                scope="global"
            ))
        
        # VAR declarations
        for match in self.PATTERNS["var_def"].finditer(content):
            variables.append(CalcVariable(
                name=match.group(1),
                value=match.group(2).strip() if match.group(2) else None,
                scope="local"
            ))
        
        return variables
    
    def _parse_script_body(self, content: str) -> tuple[list[CalcTransform], dict[str, list[str]]]:
        """Parse the main script body for transforms and member references."""
        transforms: list[CalcTransform] = []
        referenced_members: dict[str, list[str]] = {}
        
        # Track line numbers
        lines = content.split('\n')
        
        # Current FIX context stack
        fix_stack: list[dict[str, list[str]]] = []
        
        line_num = 0
        for line in lines:
            line_num += 1
            stripped = line.strip()
            
            if not stripped or stripped.startswith('/*') or stripped.startswith('//'):
                continue
            
            # Check for FIX start
            fix_match = self.PATTERNS["fix_start"].search(stripped)
            if fix_match:
                fix_members = self._parse_fix_members(fix_match.group(1))
                fix_stack.append(fix_members)
                continue
            
            # Check for ENDFIX
            if self.PATTERNS["fix_end"].match(stripped):
                if fix_stack:
                    fix_stack.pop()
                continue
            
            # Get current fix context
            current_fix = self._merge_fix_contexts(fix_stack) if fix_stack else None
            
            # Check for various operations
            transform = self._parse_operation(stripped, line_num, current_fix)
            if transform:
                transforms.append(transform)
            
            # Extract member references
            self._extract_member_references(stripped, referenced_members)
        
        return transforms, referenced_members
    
    def _parse_fix_members(self, fix_content: str) -> dict[str, list[str]]:
        """Parse FIX statement member list into dimension->members mapping."""
        members_by_dim: dict[str, list[str]] = {}
        
        # Handle dimension->member notation
        for match in self.PATTERNS["dim_member"].finditer(fix_content):
            dim = match.group(1)
            member = match.group(3)
            if dim not in members_by_dim:
                members_by_dim[dim] = []
            members_by_dim[dim].append(member)
        
        # Handle cross-dimensional references
        for match in self.PATTERNS["cross_dim"].finditer(fix_content):
            inner = match.group(1)
            # Parse inner members
            for inner_match in self.PATTERNS["dim_member"].finditer(inner):
                dim = inner_match.group(1)
                member = inner_match.group(3)
                if dim not in members_by_dim:
                    members_by_dim[dim] = []
                members_by_dim[dim].append(member)
        
        # Handle bare member names (will need dimension context to resolve)
        bare_members = []
        for match in self.PATTERNS["member_ref"].finditer(fix_content):
            member = match.group(1) or match.group(2) or match.group(3)
            if member and not any(member in v for v in members_by_dim.values()):
                bare_members.append(member)
        
        if bare_members:
            members_by_dim["_unresolved_"] = bare_members
        
        return members_by_dim
    
    def _merge_fix_contexts(self, fix_stack: list[dict[str, list[str]]]) -> dict[str, list[str]]:
        """Merge nested FIX contexts into single context."""
        merged: dict[str, list[str]] = {}
        for fix_ctx in fix_stack:
            for dim, members in fix_ctx.items():
                if dim not in merged:
                    merged[dim] = []
                merged[dim].extend(m for m in members if m not in merged[dim])
        return merged
    
    def _parse_operation(
        self,
        line: str,
        line_num: int,
        fix_context: Optional[dict[str, list[str]]]
    ) -> Optional[CalcTransform]:
        """Parse a single line for calculation operations."""
        
        # AGG operation
        agg_match = self.PATTERNS["agg"].search(line)
        if agg_match:
            dims = [d.strip().strip('"\'') for d in agg_match.group(1).split(',')]
            return CalcTransform(
                operation_type=CalcOperation.AGGREGATION,
                target_members=dims,
                fix_context=fix_context,
                line_number=line_num,
                raw_statement=line.strip(),
            )
        
        # CALC DIM operation
        calc_dim_match = self.PATTERNS["calc_dim"].search(line)
        if calc_dim_match:
            dims = [d.strip().strip('"\'') for d in calc_dim_match.group(1).split(',')]
            return CalcTransform(
                operation_type=CalcOperation.CALC_DIM,
                target_members=dims,
                fix_context=fix_context,
                line_number=line_num,
                raw_statement=line.strip(),
            )
        
        # CALC ALL
        if self.PATTERNS["calc_all"].search(line):
            return CalcTransform(
                operation_type=CalcOperation.AGGREGATION,
                target_members=["ALL"],
                fix_context=fix_context,
                line_number=line_num,
                raw_statement=line.strip(),
            )
        
        # DATACOPY operation
        datacopy_match = self.PATTERNS["datacopy"].search(line)
        if datacopy_match:
            return CalcTransform(
                operation_type=CalcOperation.DATACOPY,
                source_region=datacopy_match.group(1).strip(),
                target_region=datacopy_match.group(2).strip(),
                fix_context=fix_context,
                line_number=line_num,
                raw_statement=line.strip(),
            )
        
        # CLEARDATA operation
        cleardata_match = self.PATTERNS["cleardata"].search(line)
        if cleardata_match:
            return CalcTransform(
                operation_type=CalcOperation.CLEAR,
                target_members=[cleardata_match.group(1).strip()],
                fix_context=fix_context,
                line_number=line_num,
                raw_statement=line.strip(),
            )
        
        # Member formula assignment
        formula_match = self.PATTERNS["member_formula"].search(line)
        if formula_match:
            member = formula_match.group(1).strip().strip('"\'')
            formula = formula_match.group(2).strip()
            
            # Extract source members from formula
            source_members = self._extract_members_from_formula(formula)
            
            return CalcTransform(
                operation_type=CalcOperation.FORMULA,
                target_members=[member],
                source_members=source_members,
                formula=formula,
                fix_context=fix_context,
                line_number=line_num,
                raw_statement=line.strip(),
            )
        
        return None
    
    def _extract_members_from_formula(self, formula: str) -> list[str]:
        """Extract member references from a formula expression."""
        members = []
        
        # Quoted members
        for match in self.PATTERNS["member_ref"].finditer(formula):
            member = match.group(1) or match.group(2) or match.group(3)
            if member and member not in members:
                members.append(member)
        
        # Dimension->member references
        for match in self.PATTERNS["dim_member"].finditer(formula):
            member = f"{match.group(1)}->{match.group(3)}"
            if member not in members:
                members.append(member)
        
        return members
    
    def _extract_member_references(self, line: str, refs: dict[str, list[str]]) -> None:
        """Extract and categorize member references from a line."""
        # Dimension->member notation
        for match in self.PATTERNS["dim_member"].finditer(line):
            dim = match.group(1)
            member = match.group(3)
            if dim not in refs:
                refs[dim] = []
            if member not in refs[dim]:
                refs[dim].append(member)
        
        # Generic member references (uncategorized)
        for match in self.PATTERNS["member_ref"].finditer(line):
            member = match.group(1) or match.group(2) or match.group(3)
            if member:
                if "_uncategorized_" not in refs:
                    refs["_uncategorized_"] = []
                if member not in refs["_uncategorized_"]:
                    refs["_uncategorized_"].append(member)


class CalcScriptExtractor:
    """
    Extracts calculation scripts from Essbase and parses them.
    """
    
    def __init__(self, client: EssbaseClient):
        self.client = client
        self.parser = CalcScriptParser()
    
    def extract_all_scripts(self, app_name: str, cube_name: str) -> list[EssbaseCalcScript]:
        """Extract and parse all calc scripts for a cube."""
        scripts = []
        
        script_list = self.client.list_calc_scripts(app_name, cube_name)
        logger.info(f"Found {len(script_list)} calc scripts in {app_name}.{cube_name}")
        
        for script_info in script_list:
            script_name = script_info.get("name")
            if not script_name:
                continue
            
            try:
                script = self.extract_script(app_name, cube_name, script_name)
                if script:
                    scripts.append(script)
            except Exception as e:
                logger.error(f"Error extracting script {script_name}: {e}")
        
        return scripts
    
    def extract_script(
        self,
        app_name: str,
        cube_name: str,
        script_name: str,
    ) -> Optional[EssbaseCalcScript]:
        """Extract and parse a single calc script."""
        logger.info(f"Extracting calc script: {app_name}.{cube_name}.{script_name}")
        
        script_data = self.client.get_calc_script(app_name, cube_name, script_name)
        if not script_data:
            logger.warning(f"Script not found: {script_name}")
            return None
        
        content = script_data.get("content", "")
        if not content:
            logger.warning(f"Script has no content: {script_name}")
            return None
        
        # Parse the script
        script = self.parser.parse(content, script_name, app_name, cube_name)
        
        # Add metadata from API
        script.description = script_data.get("description")
        script.created_time = self._parse_timestamp(script_data.get("createdTime"))
        script.modified_time = self._parse_timestamp(script_data.get("modifiedTime"))
        
        return script
    
    def _parse_timestamp(self, value: Optional[str]) -> Optional[datetime]:
        """Parse ISO timestamp."""
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
