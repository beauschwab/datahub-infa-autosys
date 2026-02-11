"""
Lineage extractor for Essbase.

Builds fine-grained lineage from:
- Member formulas (dynamic calc members)
- Calculation scripts (FIX statements, AGG, DATACOPY, etc.)
- Load rules (source-to-dimension mappings)
- Member hierarchies (consolidation paths)
"""

from __future__ import annotations
import logging
import re
from typing import Optional, Any
from uuid import uuid4

from .essbase_client import EssbaseClient
from .schema_extractor import SchemaExtractor
from .calc_extractor import CalcScriptExtractor
from ..models.essbase_models import (
    EssbaseCube,
    EssbaseDimension,
    EssbaseMember,
    EssbaseCalcScript,
    EssbaseLoadRule,
    CalcTransform,
    CalcOperation,
    MemberStorage,
)
from ..models.openlineage_models import (
    ColumnLineageMapping,
    TransformOperation,
    OpenLineageJob,
    InputDataset,
    OutputDataset,
)

logger = logging.getLogger(__name__)


class LineageExtractor:
    """
    Extracts lineage information from Essbase metadata.
    
    Produces:
    - Column-level lineage mappings (source field -> target field)
    - Transform operations (detailed calculation logic)
    - DataHub-compatible DataJob definitions
    """
    
    def __init__(
        self,
        client: EssbaseClient,
        schema_extractor: Optional[SchemaExtractor] = None,
        calc_extractor: Optional[CalcScriptExtractor] = None,
    ):
        self.client = client
        self.schema_extractor = schema_extractor or SchemaExtractor(client)
        self.calc_extractor = calc_extractor or CalcScriptExtractor(client)
        self._member_dimension_cache: dict[str, str] = {}
    
    # =========================================================================
    # Member Formula Lineage
    # =========================================================================
    
    def extract_member_formula_lineage(
        self,
        cube: EssbaseCube,
        namespace: str,
    ) -> list[ColumnLineageMapping]:
        """Extract lineage from member formulas (dynamic calc members)."""
        lineage_mappings = []
        dataset_name = f"{cube.application}.{cube.name}"
        
        for dimension in cube.dimensions:
            for member in dimension.members:
                if not member.has_formula():
                    continue
                
                source_refs = self._parse_formula_references(
                    member.formula, dimension, cube
                )
                
                for source_ref in source_refs:
                    mapping = ColumnLineageMapping(
                        source_dataset=dataset_name,
                        source_field=source_ref["field_path"],
                        target_dataset=dataset_name,
                        target_field=f"{dimension.name}.{member.name}",
                        transformation_type="member_formula",
                        transformation_expression=member.formula,
                        aggregation_function=source_ref.get("aggregation"),
                    )
                    lineage_mappings.append(mapping)
        
        return lineage_mappings
    
    def _parse_formula_references(
        self,
        formula: str,
        context_dimension: EssbaseDimension,
        cube: EssbaseCube,
    ) -> list[dict[str, Any]]:
        """Parse a member formula to extract source member references."""
        references = []
        
        patterns = [
            (r'"([^"]+)"', None),
            (r"'([^']+)'", None),
            (r'\[([^\]]+)\]', None),
            (r'(\w+)->(["\']?)([^"\'\s,;()]+)\2', "dim_member"),
        ]
        
        for pattern, pattern_type in patterns:
            for match in re.finditer(pattern, formula, re.IGNORECASE):
                if pattern_type == "dim_member":
                    dim_name = match.group(1)
                    member_name = match.group(3)
                    references.append({
                        "field_path": f"{dim_name}.{member_name}",
                        "dimension": dim_name,
                        "member": member_name,
                    })
                else:
                    member_name = match.group(1)
                    dim = self._resolve_member_dimension(member_name, cube)
                    dim_name = dim.name if dim else context_dimension.name
                    references.append({
                        "field_path": f"{dim_name}.{member_name}",
                        "dimension": dim_name,
                        "member": member_name,
                    })
        
        agg_patterns = {
            r'@SUM\s*\(': 'SUM',
            r'@AVG\s*\(': 'AVG',
            r'@COUNT\s*\(': 'COUNT',
            r'@MAX\s*\(': 'MAX',
            r'@MIN\s*\(': 'MIN',
            r'@PRIOR\s*\(': 'PRIOR',
            r'@PARENTVAL\s*\(': 'PARENT_VALUE',
        }
        
        for pattern, agg_func in agg_patterns.items():
            if re.search(pattern, formula, re.IGNORECASE):
                for ref in references:
                    ref["aggregation"] = agg_func
        
        return references
    
    def _resolve_member_dimension(
        self,
        member_name: str,
        cube: EssbaseCube,
    ) -> Optional[EssbaseDimension]:
        """Find which dimension a member belongs to."""
        cache_key = f"{cube.application}.{cube.name}.{member_name}"
        
        if cache_key in self._member_dimension_cache:
            dim_name = self._member_dimension_cache[cache_key]
            return next((d for d in cube.dimensions if d.name == dim_name), None)
        
        for dimension in cube.dimensions:
            for member in dimension.members:
                if member.name == member_name:
                    self._member_dimension_cache[cache_key] = dimension.name
                    return dimension
        
        return None
    
    # =========================================================================
    # Consolidation Path Lineage
    # =========================================================================
    
    def extract_consolidation_lineage(
        self,
        cube: EssbaseCube,
        namespace: str,
    ) -> list[ColumnLineageMapping]:
        """Extract lineage from member hierarchy consolidation paths."""
        lineage_mappings = []
        dataset_name = f"{cube.application}.{cube.name}"
        
        for dimension in cube.dimensions:
            member_map = {m.name: m for m in dimension.members}
            
            for member in dimension.members:
                if not member.children:
                    continue
                
                for child_name in member.children:
                    child = member_map.get(child_name)
                    if not child:
                        continue
                    
                    transform_type = self._consolidation_to_transform(child.consolidation.value)
                    
                    mapping = ColumnLineageMapping(
                        source_dataset=dataset_name,
                        source_field=f"{dimension.name}.{child_name}",
                        target_dataset=dataset_name,
                        target_field=f"{dimension.name}.{member.name}",
                        transformation_type="consolidation",
                        transformation_expression=f"{child.consolidation.value}{child_name}",
                        aggregation_function=transform_type,
                    )
                    lineage_mappings.append(mapping)
        
        return lineage_mappings
    
    def _consolidation_to_transform(self, operator: str) -> str:
        """Map consolidation operator to aggregation function."""
        mapping = {
            "+": "SUM",
            "-": "SUBTRACT",
            "*": "MULTIPLY",
            "/": "DIVIDE",
            "%": "PERCENT",
            "~": "IGNORE",
            "^": "NEVER_CONSOLIDATE",
        }
        return mapping.get(operator, "SUM")
    
    # =========================================================================
    # Calculation Script Lineage
    # =========================================================================
    
    def extract_calc_script_lineage(
        self,
        app_name: str,
        cube_name: str,
        namespace: str,
    ) -> tuple[list[OpenLineageJob], list[ColumnLineageMapping]]:
        """Extract lineage from calculation scripts."""
        jobs = []
        all_lineage = []
        dataset_name = f"{app_name}.{cube_name}"
        
        scripts = self.calc_extractor.extract_all_scripts(app_name, cube_name)
        
        for script in scripts:
            job, lineage = self._calc_script_to_job(script, namespace, dataset_name)
            jobs.append(job)
            all_lineage.extend(lineage)
        
        return jobs, all_lineage
    
    def _calc_script_to_job(
        self,
        script: EssbaseCalcScript,
        namespace: str,
        dataset_name: str,
    ) -> tuple[OpenLineageJob, list[ColumnLineageMapping]]:
        """Convert a calc script to an OpenLineage job with lineage."""
        lineage_mappings = []
        transform_ops = []
        
        for transform in script.transforms:
            op_id = f"{script.name}_{transform.line_number or uuid4().hex[:8]}"
            
            op = TransformOperation(
                operation_id=op_id,
                operation_type=transform.operation_type.value,
                name=f"{transform.operation_type.value}@L{transform.line_number}",
                logic_expression=transform.formula or transform.raw_statement,
                fix_context=transform.fix_context,
                line_range=(transform.line_number, transform.line_number) if transform.line_number else None,
            )
            
            if transform.operation_type == CalcOperation.FORMULA:
                for source in transform.source_members:
                    for target in transform.target_members:
                        lineage_mappings.append(ColumnLineageMapping(
                            source_dataset=dataset_name,
                            source_field=source,
                            target_dataset=dataset_name,
                            target_field=target,
                            transformation_type="calc_script_formula",
                            transformation_expression=transform.formula,
                            filter_expression=self._fix_context_to_filter(transform.fix_context),
                        ))
                        op.input_fields.append(ColumnLineageMapping(
                            source_dataset=dataset_name,
                            source_field=source,
                            target_dataset=dataset_name,
                            target_field=target,
                        ))
                op.output_fields = transform.target_members
            
            elif transform.operation_type == CalcOperation.DATACOPY:
                lineage_mappings.append(ColumnLineageMapping(
                    source_dataset=dataset_name,
                    source_field=transform.source_region or "",
                    target_dataset=dataset_name,
                    target_field=transform.target_region or "",
                    transformation_type="datacopy",
                    transformation_expression=transform.raw_statement,
                    filter_expression=self._fix_context_to_filter(transform.fix_context),
                ))
            
            elif transform.operation_type in [CalcOperation.AGGREGATION, CalcOperation.CALC_DIM]:
                for dim in transform.target_members:
                    lineage_mappings.append(ColumnLineageMapping(
                        source_dataset=dataset_name,
                        source_field=f"{dim}.*",
                        target_dataset=dataset_name,
                        target_field=f"{dim}.*",
                        transformation_type=transform.operation_type.value,
                        aggregation_function="CONSOLIDATE",
                        filter_expression=self._fix_context_to_filter(transform.fix_context),
                    ))
            
            transform_ops.append(op)
        
        job = OpenLineageJob(
            namespace=namespace,
            name=f"{script.application}.{script.cube}.{script.name}",
            platform="essbase",
            flow_urn=f"{script.application}.{script.cube}",
            job_type="essbase_calculation",
            description=script.description,
            inputs=[InputDataset(namespace=namespace, name=dataset_name)],
            outputs=[OutputDataset(namespace=namespace, name=dataset_name)],
            column_lineage=lineage_mappings,
            transform_operations=transform_ops,
            source_code=script.content,
            custom_properties={
                "script_name": script.name,
                "application": script.application,
                "cube": script.cube,
                "variable_count": str(len(script.variables)),
                "transform_count": str(len(script.transforms)),
            },
            tags=["essbase", "calculation_script"],
        )
        
        return job, lineage_mappings
    
    def _fix_context_to_filter(self, fix_context: Optional[dict[str, list[str]]]) -> Optional[str]:
        """Convert FIX context to filter expression string."""
        if not fix_context:
            return None
        
        parts = []
        for dim, members in fix_context.items():
            if dim == "_unresolved_":
                continue
            member_list = ", ".join(f'"{m}"' for m in members)
            parts.append(f"{dim} IN ({member_list})")
        
        return " AND ".join(parts) if parts else None
    
    # =========================================================================
    # Load Rule Lineage
    # =========================================================================
    
    def extract_load_rule_lineage(
        self,
        app_name: str,
        cube_name: str,
        namespace: str,
    ) -> tuple[list[OpenLineageJob], list[ColumnLineageMapping]]:
        """Extract lineage from load rules."""
        jobs = []
        all_lineage = []
        
        for rule_type in ["data", "dimension"]:
            rules = self.client.list_load_rules(app_name, cube_name, rule_type)
            
            for rule_info in rules:
                rule_name = rule_info.get("name")
                if not rule_name:
                    continue
                
                try:
                    rule_data = self.client.get_load_rule(
                        app_name, cube_name, rule_name, rule_type
                    )
                    if rule_data:
                        job, lineage = self._load_rule_to_job(
                            rule_data, app_name, cube_name, namespace, rule_type
                        )
                        jobs.append(job)
                        all_lineage.extend(lineage)
                except Exception as e:
                    logger.error(f"Error processing load rule {rule_name}: {e}")
        
        return jobs, all_lineage
    
    def _load_rule_to_job(
        self,
        rule_data: dict,
        app_name: str,
        cube_name: str,
        namespace: str,
        rule_type: str,
    ) -> tuple[OpenLineageJob, list[ColumnLineageMapping]]:
        """Convert a load rule to an OpenLineage job."""
        rule_name = rule_data.get("name", "unknown")
        target_dataset = f"{app_name}.{cube_name}"
        lineage_mappings = []
        
        source_dataset = rule_data.get("dataSource", {}).get("name", "external_source")
        source_type = rule_data.get("dataSource", {}).get("type", "file")
        
        if source_type == "sql":
            source_dataset = f"sql://{rule_data.get('dataSource', {}).get('connection', 'unknown')}"
        
        field_mappings = rule_data.get("fieldMappings", [])
        for mapping in field_mappings:
            field_num = mapping.get("fieldNumber", 0)
            target_dim = mapping.get("dimension", "")
            
            source_field = mapping.get("sourceField", f"field_{field_num}")
            
            lineage_mappings.append(ColumnLineageMapping(
                source_dataset=source_dataset,
                source_field=source_field,
                target_dataset=target_dataset,
                target_field=target_dim,
                transformation_type=f"load_rule_{rule_type}",
                transformation_expression=mapping.get("transformation"),
            ))
        
        job = OpenLineageJob(
            namespace=namespace,
            name=f"{app_name}.{cube_name}.{rule_name}",
            platform="essbase",
            flow_urn=f"{app_name}.{cube_name}",
            job_type=f"essbase_{rule_type}_load",
            description=rule_data.get("description"),
            inputs=[InputDataset(namespace=namespace, name=source_dataset)],
            outputs=[OutputDataset(namespace=namespace, name=target_dataset)],
            column_lineage=lineage_mappings,
            custom_properties={
                "rule_name": rule_name,
                "rule_type": rule_type,
                "source_type": source_type,
                "application": app_name,
                "cube": cube_name,
            },
            tags=["essbase", f"{rule_type}_load_rule"],
        )
        
        return job, lineage_mappings
    
    # =========================================================================
    # Complete Extraction
    # =========================================================================
    
    def extract_all_lineage(
        self,
        app_name: str,
        cube_name: str,
        namespace: str,
        include_consolidation: bool = True,
        include_formulas: bool = True,
        include_calc_scripts: bool = True,
        include_load_rules: bool = True,
    ) -> dict[str, Any]:
        """
        Extract all lineage for a cube.
        
        Returns dict with:
        - jobs: List of OpenLineageJob
        - column_lineage: List of ColumnLineageMapping
        - transform_operations: List of TransformOperation
        """
        jobs = []
        all_lineage = []
        
        cube = self.schema_extractor.extract_cube(app_name, cube_name)
        
        if include_formulas:
            formula_lineage = self.extract_member_formula_lineage(cube, namespace)
            all_lineage.extend(formula_lineage)
            logger.info(f"Extracted {len(formula_lineage)} member formula lineage mappings")
        
        if include_consolidation:
            consol_lineage = self.extract_consolidation_lineage(cube, namespace)
            all_lineage.extend(consol_lineage)
            logger.info(f"Extracted {len(consol_lineage)} consolidation lineage mappings")
        
        if include_calc_scripts:
            calc_jobs, calc_lineage = self.extract_calc_script_lineage(
                app_name, cube_name, namespace
            )
            jobs.extend(calc_jobs)
            all_lineage.extend(calc_lineage)
            logger.info(f"Extracted {len(calc_jobs)} calc script jobs, {len(calc_lineage)} lineage mappings")
        
        if include_load_rules:
            load_jobs, load_lineage = self.extract_load_rule_lineage(
                app_name, cube_name, namespace
            )
            jobs.extend(load_jobs)
            all_lineage.extend(load_lineage)
            logger.info(f"Extracted {len(load_jobs)} load rule jobs, {len(load_lineage)} lineage mappings")
        
        return {
            "jobs": jobs,
            "column_lineage": all_lineage,
            "cube": cube,
        }
