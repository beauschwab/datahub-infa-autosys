"""
Informatica PowerCenter to DataHub Lineage Integration

This module parses Informatica PowerCenter XML exports and emits complete
column-level lineage to DataHub, handling:
- Standard transformation types (Expression, Aggregator, Filter, Joiner, etc.)
- Source Qualifier SQL overrides with SQL parsing for column lineage
- Oracle stored procedure calls as cross-platform entity references
- Ordered transformation chains through intermediate DataJob entities

Usage:
    from informatica_lineage import InformaticaLineageExtractor
    
    extractor = InformaticaLineageExtractor(
        datahub_server="http://localhost:8080",
        platform_instance="prod",
        oracle_platform_instance="prod-oracle"
    )
    extractor.process_xml_file("workflow_export.xml")
    extractor.emit_to_datahub()
"""

from __future__ import annotations

import hashlib
import logging
import re
import uuid
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, Union

# DataHub imports
from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_dataset_urn,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)
from datahub.metadata.com.linkedin.pegasus2avro.datajob import (
    DataFlowInfo,
    DataJobInfo,
    DataJobInputOutput,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringType,
    NumberType,
    DateType,
    BooleanType,
)

# SQL parsing
try:
    import sqlglot
    from sqlglot import exp
    from sqlglot.lineage import lineage as sqlglot_lineage
    SQL_PARSING_AVAILABLE = True
except ImportError:
    SQL_PARSING_AVAILABLE = False
    logging.warning("sqlglot not installed - SQL parsing will be disabled")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =============================================================================
# Data Models
# =============================================================================

class TransformationType(Enum):
    """Informatica transformation types with lineage semantics."""
    SOURCE_DEFINITION = "Source Definition"
    SOURCE_QUALIFIER = "Source Qualifier"
    TARGET_DEFINITION = "Target Definition"
    EXPRESSION = "Expression"
    FILTER = "Filter"
    AGGREGATOR = "Aggregator"
    JOINER = "Joiner"
    LOOKUP = "Lookup Procedure"
    ROUTER = "Router"
    SORTER = "Sorter"
    SEQUENCE_GENERATOR = "Sequence Generator"
    NORMALIZER = "Normalizer"
    RANK = "Rank"
    UNION = "Union"
    UPDATE_STRATEGY = "Update Strategy"
    STORED_PROCEDURE = "Stored Procedure"
    CUSTOM_TRANSFORMATION = "Custom Transformation"
    UNKNOWN = "Unknown"
    
    @classmethod
    def from_string(cls, value: str) -> "TransformationType":
        """Convert string to TransformationType with fallback."""
        for member in cls:
            if member.value.lower() == value.lower():
                return member
        return cls.UNKNOWN


@dataclass
class FieldInfo:
    """Represents a field/column in a source, target, or transformation."""
    name: str
    datatype: str
    precision: Optional[int] = None
    scale: Optional[int] = None
    port_type: Optional[str] = None  # INPUT, OUTPUT, INPUT/OUTPUT, LOCAL
    expression: Optional[str] = None
    default_value: Optional[str] = None
    is_nullable: bool = True
    description: Optional[str] = None
    

@dataclass
class ConnectorInfo:
    """Represents a CONNECTOR element linking fields between transformations."""
    from_instance: str
    from_instance_type: str
    from_field: str
    to_instance: str
    to_instance_type: str
    to_field: str
    

@dataclass
class TransformationInfo:
    """Represents an Informatica transformation (reusable or within a mapping)."""
    name: str
    type: TransformationType
    fields: Dict[str, FieldInfo] = field(default_factory=dict)
    sql_query: Optional[str] = None  # For Source Qualifier SQL override
    stored_proc_name: Optional[str] = None  # For Stored Procedure calls
    stored_proc_schema: Optional[str] = None
    filter_condition: Optional[str] = None
    joiner_condition: Optional[str] = None
    lookup_condition: Optional[str] = None
    properties: Dict[str, str] = field(default_factory=dict)
    

@dataclass
class SourceDefinition:
    """Represents an Informatica SOURCE element."""
    name: str
    database_name: Optional[str] = None
    database_type: Optional[str] = None
    owner_name: Optional[str] = None
    fields: Dict[str, FieldInfo] = field(default_factory=dict)
    

@dataclass
class TargetDefinition:
    """Represents an Informatica TARGET element."""
    name: str
    database_name: Optional[str] = None
    database_type: Optional[str] = None
    owner_name: Optional[str] = None
    fields: Dict[str, FieldInfo] = field(default_factory=dict)
    

@dataclass
class InstanceInfo:
    """Represents an INSTANCE element within a mapping."""
    name: str
    transformation_name: str
    transformation_type: TransformationType
    reusable: bool = False
    description: Optional[str] = None
    

@dataclass 
class MappingInfo:
    """Represents an Informatica MAPPING element."""
    name: str
    description: Optional[str] = None
    is_valid: bool = True
    instances: Dict[str, InstanceInfo] = field(default_factory=dict)
    connectors: List[ConnectorInfo] = field(default_factory=list)
    transformations: Dict[str, TransformationInfo] = field(default_factory=dict)
    

@dataclass
class SessionInfo:
    """Represents an Informatica SESSION element."""
    name: str
    mapping_name: str
    is_valid: bool = True
    description: Optional[str] = None
    source_connections: Dict[str, str] = field(default_factory=dict)  # instance -> connection
    target_connections: Dict[str, str] = field(default_factory=dict)
    properties: Dict[str, str] = field(default_factory=dict)
    

@dataclass
class WorkflowInfo:
    """Represents an Informatica WORKFLOW element."""
    name: str
    description: Optional[str] = None
    is_enabled: bool = True
    sessions: List[str] = field(default_factory=list)  # Session names in execution order
    

@dataclass
class FolderInfo:
    """Represents an Informatica FOLDER element containing all objects."""
    name: str
    sources: Dict[str, SourceDefinition] = field(default_factory=dict)
    targets: Dict[str, TargetDefinition] = field(default_factory=dict)
    transformations: Dict[str, TransformationInfo] = field(default_factory=dict)
    mappings: Dict[str, MappingInfo] = field(default_factory=dict)
    sessions: Dict[str, SessionInfo] = field(default_factory=dict)
    workflows: Dict[str, WorkflowInfo] = field(default_factory=dict)


@dataclass
class ColumnLineageEdge:
    """Represents a single column-to-column lineage relationship."""
    source_dataset: str
    source_column: str
    target_dataset: str
    target_column: str
    transformation_type: TransformationType
    transformation_expression: Optional[str] = None
    confidence_score: float = 1.0
    step_order: int = 0  # Order in transformation chain


@dataclass
class TransformationStep:
    """Represents one step in an ordered transformation chain."""
    step_order: int
    instance_name: str
    transformation_type: TransformationType
    input_fields: List[Tuple[str, str]]  # (instance, field) pairs
    output_fields: List[Tuple[str, str]]  # (instance, field) pairs
    expression: Optional[str] = None
    

# =============================================================================
# XML Parser
# =============================================================================

class InformaticaXMLParser:
    """Parses Informatica PowerCenter XML export files."""
    
    def __init__(self):
        self.folders: Dict[str, FolderInfo] = {}
        self.repository_name: Optional[str] = None
        
    def parse_file(self, xml_path: Union[str, Path]) -> Dict[str, FolderInfo]:
        """Parse an Informatica XML export file."""
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        if root.tag != "POWERMART":
            raise ValueError(f"Expected POWERMART root element, got {root.tag}")
            
        for repository in root.findall("REPOSITORY"):
            self.repository_name = repository.get("NAME")
            self._parse_repository(repository)
            
        return self.folders
    
    def parse_string(self, xml_content: str) -> Dict[str, FolderInfo]:
        """Parse Informatica XML from a string."""
        root = ET.fromstring(xml_content)
        
        if root.tag != "POWERMART":
            raise ValueError(f"Expected POWERMART root element, got {root.tag}")
            
        for repository in root.findall("REPOSITORY"):
            self.repository_name = repository.get("NAME")
            self._parse_repository(repository)
            
        return self.folders
    
    def _parse_repository(self, repository: ET.Element):
        """Parse all folders in a repository."""
        for folder_elem in repository.findall("FOLDER"):
            folder = self._parse_folder(folder_elem)
            self.folders[folder.name] = folder
            
    def _parse_folder(self, folder_elem: ET.Element) -> FolderInfo:
        """Parse a FOLDER element with all its contents."""
        folder = FolderInfo(name=folder_elem.get("NAME", ""))
        
        # Parse sources
        for source in folder_elem.findall("SOURCE"):
            src = self._parse_source(source)
            folder.sources[src.name] = src
            
        # Parse targets
        for target in folder_elem.findall("TARGET"):
            tgt = self._parse_target(target)
            folder.targets[tgt.name] = tgt
            
        # Parse reusable transformations
        for transform in folder_elem.findall("TRANSFORMATION"):
            trans = self._parse_transformation(transform)
            folder.transformations[trans.name] = trans
            
        # Parse mappings
        for mapping in folder_elem.findall("MAPPING"):
            map_info = self._parse_mapping(mapping)
            folder.mappings[map_info.name] = map_info
            
        # Parse sessions
        for session in folder_elem.findall("SESSION"):
            sess = self._parse_session(session)
            folder.sessions[sess.name] = sess
            
        # Parse workflows
        for workflow in folder_elem.findall("WORKFLOW"):
            wf = self._parse_workflow(workflow)
            folder.workflows[wf.name] = wf
            
        return folder
    
    def _parse_source(self, source_elem: ET.Element) -> SourceDefinition:
        """Parse a SOURCE element."""
        source = SourceDefinition(
            name=source_elem.get("NAME", ""),
            database_name=source_elem.get("DBDNAME"),
            database_type=source_elem.get("DATABASETYPE"),
            owner_name=source_elem.get("OWNERNAME"),
        )
        
        for field_elem in source_elem.findall("SOURCEFIELD"):
            field_info = FieldInfo(
                name=field_elem.get("NAME", ""),
                datatype=field_elem.get("DATATYPE", "string"),
                precision=self._safe_int(field_elem.get("PRECISION")),
                scale=self._safe_int(field_elem.get("SCALE")),
                is_nullable=field_elem.get("NULLABLE", "NULL") == "NULL",
                description=field_elem.get("DESCRIPTION"),
            )
            source.fields[field_info.name] = field_info
            
        return source
    
    def _parse_target(self, target_elem: ET.Element) -> TargetDefinition:
        """Parse a TARGET element."""
        target = TargetDefinition(
            name=target_elem.get("NAME", ""),
            database_name=target_elem.get("DBDNAME"),
            database_type=target_elem.get("DATABASETYPE"),
            owner_name=target_elem.get("OWNERNAME"),
        )
        
        for field_elem in target_elem.findall("TARGETFIELD"):
            field_info = FieldInfo(
                name=field_elem.get("NAME", ""),
                datatype=field_elem.get("DATATYPE", "string"),
                precision=self._safe_int(field_elem.get("PRECISION")),
                scale=self._safe_int(field_elem.get("SCALE")),
                is_nullable=field_elem.get("NULLABLE", "NULL") == "NULL",
            )
            target.fields[field_info.name] = field_info
            
        return target
    
    def _parse_transformation(self, trans_elem: ET.Element) -> TransformationInfo:
        """Parse a TRANSFORMATION element."""
        trans_type = TransformationType.from_string(trans_elem.get("TYPE", ""))
        
        transformation = TransformationInfo(
            name=trans_elem.get("NAME", ""),
            type=trans_type,
        )
        
        # Parse transformation fields
        for field_elem in trans_elem.findall("TRANSFORMFIELD"):
            field_info = FieldInfo(
                name=field_elem.get("NAME", ""),
                datatype=field_elem.get("DATATYPE", "string"),
                precision=self._safe_int(field_elem.get("PRECISION")),
                scale=self._safe_int(field_elem.get("SCALE")),
                port_type=field_elem.get("PORTTYPE"),
                expression=field_elem.get("EXPRESSION"),
                default_value=field_elem.get("DEFAULTVALUE"),
            )
            transformation.fields[field_info.name] = field_info
            
        # Extract SQL query for Source Qualifier
        if trans_type == TransformationType.SOURCE_QUALIFIER:
            transformation.sql_query = self._extract_sql_query(trans_elem)
            
        # Extract stored procedure info
        if trans_type == TransformationType.STORED_PROCEDURE:
            transformation.stored_proc_name = trans_elem.get("PROCEDURENAME")
            transformation.stored_proc_schema = trans_elem.get("OWNERNAME")
            
        # Extract filter condition
        for table_attr in trans_elem.findall("TABLEATTRIBUTE"):
            attr_name = table_attr.get("NAME", "")
            attr_value = table_attr.get("VALUE", "")
            
            if attr_name == "Sql Query":
                transformation.sql_query = attr_value
            elif attr_name == "Filter Condition":
                transformation.filter_condition = attr_value
            elif attr_name == "Join Condition":
                transformation.joiner_condition = attr_value
            elif attr_name == "Lookup condition":
                transformation.lookup_condition = attr_value
            elif attr_name == "Stored Procedure Name":
                transformation.stored_proc_name = attr_value
                
            transformation.properties[attr_name] = attr_value
            
        return transformation
    
    def _extract_sql_query(self, trans_elem: ET.Element) -> Optional[str]:
        """Extract SQL query from Source Qualifier transformation."""
        # Check TABLEATTRIBUTE for SQL Query
        for attr in trans_elem.findall("TABLEATTRIBUTE"):
            if attr.get("NAME") == "Sql Query":
                return attr.get("VALUE")
                
        # Check for User Defined Join
        for attr in trans_elem.findall("TABLEATTRIBUTE"):
            if attr.get("NAME") == "User Defined Join":
                return attr.get("VALUE")
                
        return None
    
    def _parse_mapping(self, mapping_elem: ET.Element) -> MappingInfo:
        """Parse a MAPPING element."""
        mapping = MappingInfo(
            name=mapping_elem.get("NAME", ""),
            description=mapping_elem.get("DESCRIPTION"),
            is_valid=mapping_elem.get("ISVALID", "YES") == "YES",
        )
        
        # Parse instances
        for instance in mapping_elem.findall("INSTANCE"):
            inst = InstanceInfo(
                name=instance.get("NAME", ""),
                transformation_name=instance.get("TRANSFORMATION_NAME", ""),
                transformation_type=TransformationType.from_string(
                    instance.get("TRANSFORMATION_TYPE", "")
                ),
                reusable=instance.get("REUSABLE", "NO") == "YES",
                description=instance.get("DESCRIPTION"),
            )
            mapping.instances[inst.name] = inst
            
        # Parse connectors
        for connector in mapping_elem.findall("CONNECTOR"):
            conn = ConnectorInfo(
                from_instance=connector.get("FROMINSTANCE", ""),
                from_instance_type=connector.get("FROMINSTANCETYPE", ""),
                from_field=connector.get("FROMFIELD", ""),
                to_instance=connector.get("TOINSTANCE", ""),
                to_instance_type=connector.get("TOINSTANCETYPE", ""),
                to_field=connector.get("TOFIELD", ""),
            )
            mapping.connectors.append(conn)
            
        # Parse inline transformations within mapping
        for transform in mapping_elem.findall("TRANSFORMATION"):
            trans = self._parse_transformation(transform)
            mapping.transformations[trans.name] = trans
            
        return mapping
    
    def _parse_session(self, session_elem: ET.Element) -> SessionInfo:
        """Parse a SESSION element."""
        session = SessionInfo(
            name=session_elem.get("NAME", ""),
            mapping_name=session_elem.get("MAPPINGNAME", ""),
            is_valid=session_elem.get("ISVALID", "YES") == "YES",
            description=session_elem.get("DESCRIPTION"),
        )
        
        # Parse session transformation instances for connection overrides
        for sess_trans in session_elem.findall("SESSTRANSFORMATIONINST"):
            trans_name = sess_trans.get("TRANSFORMATIONNAME", "")
            
            # Look for connection assignments
            for sess_comp in sess_trans.findall("SESSIONCOMPONENT"):
                conn_name = sess_comp.get("REFCONNECTIONNAME")
                if conn_name:
                    if "Source" in sess_trans.get("TRANSFORMATIONTYPE", ""):
                        session.source_connections[trans_name] = conn_name
                    elif "Target" in sess_trans.get("TRANSFORMATIONTYPE", ""):
                        session.target_connections[trans_name] = conn_name
                        
        # Parse session attributes
        for attr in session_elem.findall("ATTRIBUTE"):
            session.properties[attr.get("NAME", "")] = attr.get("VALUE", "")
            
        return session
    
    def _parse_workflow(self, workflow_elem: ET.Element) -> WorkflowInfo:
        """Parse a WORKFLOW element."""
        workflow = WorkflowInfo(
            name=workflow_elem.get("NAME", ""),
            description=workflow_elem.get("DESCRIPTION"),
            is_enabled=workflow_elem.get("ISENABLED", "YES") == "YES",
        )
        
        # Extract sessions from TASKINSTANCE elements
        task_order: Dict[str, int] = {}
        for task in workflow_elem.findall("TASKINSTANCE"):
            task_name = task.get("NAME", "")
            task_type = task.get("TASKTYPE", "")
            if task_type == "Session":
                task_order[task_name] = len(task_order)
                
        # Sort sessions by workflow link order if available
        workflow_links = workflow_elem.findall("WORKFLOWLINK")
        if workflow_links:
            visited = set()
            ordered_sessions = []
            
            # Build adjacency from WORKFLOWLINK
            next_tasks: Dict[str, List[str]] = {}
            for link in workflow_links:
                from_task = link.get("FROMTASK", "")
                to_task = link.get("TOTASK", "")
                if from_task not in next_tasks:
                    next_tasks[from_task] = []
                next_tasks[from_task].append(to_task)
                
            # Find start task
            all_to_tasks = {link.get("TOTASK") for link in workflow_links}
            start_tasks = [t for t in task_order.keys() if t not in all_to_tasks]
            
            # Traverse in order
            queue = start_tasks
            while queue:
                current = queue.pop(0)
                if current in visited:
                    continue
                visited.add(current)
                if current in task_order:
                    ordered_sessions.append(current)
                for next_task in next_tasks.get(current, []):
                    queue.append(next_task)
                    
            workflow.sessions = ordered_sessions if ordered_sessions else list(task_order.keys())
        else:
            workflow.sessions = list(task_order.keys())
            
        return workflow
    
    @staticmethod
    def _safe_int(value: Optional[str]) -> Optional[int]:
        """Safely convert string to int."""
        if value is None:
            return None
        try:
            return int(value)
        except ValueError:
            return None


# =============================================================================
# SQL Lineage Parser
# =============================================================================

class SQLLineageParser:
    """Parses SQL queries to extract column-level lineage using sqlglot."""
    
    def __init__(self, dialect: str = "oracle"):
        self.dialect = dialect
        
    def parse_sql(
        self,
        sql: str,
        source_tables: Dict[str, Dict[str, FieldInfo]],
        default_schema: Optional[str] = None,
    ) -> List[Tuple[List[Tuple[str, str]], str, Optional[str]]]:
        """
        Parse SQL and extract column lineage.
        
        Args:
            sql: The SQL query to parse
            source_tables: Dict mapping table names to their fields
            default_schema: Default schema for unqualified table references
            
        Returns:
            List of tuples: (upstream_columns, downstream_column, expression)
            where upstream_columns is list of (table, column) tuples
        """
        if not SQL_PARSING_AVAILABLE:
            logger.warning("SQL parsing unavailable - returning empty lineage")
            return []
            
        try:
            return self._parse_with_sqlglot(sql, source_tables, default_schema)
        except Exception as e:
            logger.error(f"SQL parsing failed: {e}")
            return []
    
    def _parse_with_sqlglot(
        self,
        sql: str,
        source_tables: Dict[str, Dict[str, FieldInfo]],
        default_schema: Optional[str] = None,
    ) -> List[Tuple[List[Tuple[str, str]], str, Optional[str]]]:
        """Parse SQL using sqlglot."""
        results = []
        
        try:
            # Parse the SQL statement
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
            
            if not isinstance(parsed, exp.Select):
                logger.warning(f"SQL is not a SELECT statement, skipping lineage")
                return []
                
            # Get all source tables referenced
            source_refs = {}
            for table in parsed.find_all(exp.Table):
                table_name = table.name
                alias = table.alias or table_name
                
                # Try to match with known source tables
                matched_table = None
                for known_table in source_tables.keys():
                    if known_table.upper() == table_name.upper():
                        matched_table = known_table
                        break
                    if known_table.upper().endswith(f".{table_name.upper()}"):
                        matched_table = known_table
                        break
                        
                if matched_table:
                    source_refs[alias.upper()] = matched_table
                else:
                    source_refs[alias.upper()] = table_name
                    
            # Analyze SELECT expressions
            for i, select_expr in enumerate(parsed.expressions):
                # Get output column name
                if isinstance(select_expr, exp.Alias):
                    output_col = select_expr.alias
                    inner_expr = select_expr.this
                elif isinstance(select_expr, exp.Column):
                    output_col = select_expr.name
                    inner_expr = select_expr
                else:
                    output_col = f"expr_{i}"
                    inner_expr = select_expr
                    
                # Find all column references in this expression
                upstream_cols = []
                expr_text = select_expr.sql(dialect=self.dialect)
                
                for col in inner_expr.find_all(exp.Column):
                    col_name = col.name
                    table_ref = col.table.upper() if col.table else None
                    
                    # Resolve table reference
                    if table_ref and table_ref in source_refs:
                        resolved_table = source_refs[table_ref]
                    elif table_ref:
                        resolved_table = table_ref
                    elif len(source_refs) == 1:
                        resolved_table = list(source_refs.values())[0]
                    else:
                        # Try to find which table has this column
                        resolved_table = self._resolve_column_table(
                            col_name, source_tables
                        )
                        
                    if resolved_table:
                        upstream_cols.append((resolved_table, col_name))
                        
                # Handle SELECT *
                if isinstance(select_expr, exp.Star):
                    for table_name, fields in source_tables.items():
                        for field_name in fields.keys():
                            results.append(([(table_name, field_name)], field_name, None))
                    continue
                    
                if upstream_cols:
                    results.append((upstream_cols, output_col, expr_text))
                    
        except sqlglot.errors.ParseError as e:
            logger.error(f"SQLGlot parse error: {e}")
            
        return results
    
    def _resolve_column_table(
        self,
        column_name: str,
        source_tables: Dict[str, Dict[str, FieldInfo]],
    ) -> Optional[str]:
        """Resolve which table a column belongs to."""
        candidates = []
        for table_name, fields in source_tables.items():
            for field_name in fields.keys():
                if field_name.upper() == column_name.upper():
                    candidates.append(table_name)
                    
        if len(candidates) == 1:
            return candidates[0]
        return None
    
    def detect_stored_procedure_call(self, sql: str) -> Optional[Tuple[str, str]]:
        """
        Detect if SQL contains a stored procedure call.
        
        Returns:
            Tuple of (schema, procedure_name) if found, None otherwise
        """
        # Common patterns for stored procedure calls
        patterns = [
            r"CALL\s+(?:(\w+)\.)?(\w+)\s*\(",
            r"EXECUTE\s+(?:(\w+)\.)?(\w+)\s*",
            r"BEGIN\s+(?:(\w+)\.)?(\w+)\s*\(",
            r"{\s*call\s+(?:(\w+)\.)?(\w+)\s*\(",
        ]
        
        for pattern in patterns:
            match = re.search(pattern, sql, re.IGNORECASE)
            if match:
                schema = match.group(1) or ""
                proc_name = match.group(2)
                return (schema, proc_name)
                
        return None


# =============================================================================
# Lineage Builder
# =============================================================================

class LineageBuilder:
    """Builds lineage from parsed Informatica metadata."""
    
    def __init__(
        self,
        folder: FolderInfo,
        platform: str = "oracle",
        platform_instance: Optional[str] = None,
        env: str = "PROD",
        oracle_platform_instance: Optional[str] = None,
    ):
        self.folder = folder
        self.platform = platform
        self.platform_instance = platform_instance
        self.env = env
        self.oracle_platform_instance = oracle_platform_instance or platform_instance
        self.sql_parser = SQLLineageParser(dialect="oracle")
        
    def build_mapping_lineage(
        self,
        mapping: MappingInfo,
    ) -> Dict[str, List[TransformationStep]]:
        """
        Build ordered transformation chains for each target field.
        
        Returns:
            Dict mapping target field URNs to their transformation chains
        """
        lineage_chains: Dict[str, List[TransformationStep]] = {}
        
        # Build graph of transformations
        instance_graph = self._build_instance_graph(mapping)
        
        # Find target instances
        target_instances = [
            inst for inst in mapping.instances.values()
            if inst.transformation_type == TransformationType.TARGET_DEFINITION
        ]
        
        for target_inst in target_instances:
            target_name = target_inst.transformation_name
            target_def = self.folder.targets.get(target_name)
            
            if not target_def:
                continue
                
            for field_name in target_def.fields.keys():
                field_key = f"{target_name}.{field_name}"
                chain = self._trace_field_lineage(
                    mapping, target_inst.name, field_name, instance_graph
                )
                if chain:
                    lineage_chains[field_key] = chain
                    
        return lineage_chains
    
    def _build_instance_graph(
        self,
        mapping: MappingInfo,
    ) -> Dict[str, Dict[str, List[Tuple[str, str]]]]:
        """
        Build a graph of field connections between instances.
        
        Returns:
            Dict[to_instance][to_field] -> List[(from_instance, from_field)]
        """
        graph: Dict[str, Dict[str, List[Tuple[str, str]]]] = {}
        
        for connector in mapping.connectors:
            to_inst = connector.to_instance
            to_field = connector.to_field
            from_inst = connector.from_instance
            from_field = connector.from_field
            
            if to_inst not in graph:
                graph[to_inst] = {}
            if to_field not in graph[to_inst]:
                graph[to_inst][to_field] = []
                
            graph[to_inst][to_field].append((from_inst, from_field))
            
        return graph
    
    def _trace_field_lineage(
        self,
        mapping: MappingInfo,
        target_instance: str,
        target_field: str,
        instance_graph: Dict[str, Dict[str, List[Tuple[str, str]]]],
        visited: Optional[Set[str]] = None,
        step_order: int = 0,
    ) -> List[TransformationStep]:
        """Recursively trace lineage back from a target field."""
        if visited is None:
            visited = set()
            
        visit_key = f"{target_instance}.{target_field}"
        if visit_key in visited:
            return []
        visited.add(visit_key)
        
        steps = []
        instance_info = mapping.instances.get(target_instance)
        
        if not instance_info:
            return []
            
        # Get upstream connections for this field
        upstream_fields = instance_graph.get(target_instance, {}).get(target_field, [])
        
        # Get transformation info
        trans = self._get_transformation(mapping, instance_info)
        expression = None
        
        if trans and target_field in trans.fields:
            expression = trans.fields[target_field].expression
            
        # Create step for this transformation
        step = TransformationStep(
            step_order=step_order,
            instance_name=target_instance,
            transformation_type=instance_info.transformation_type,
            input_fields=upstream_fields,
            output_fields=[(target_instance, target_field)],
            expression=expression,
        )
        steps.append(step)
        
        # Recursively trace upstream
        for from_inst, from_field in upstream_fields:
            upstream_steps = self._trace_field_lineage(
                mapping, from_inst, from_field, instance_graph,
                visited, step_order + 1
            )
            steps.extend(upstream_steps)
            
        return steps
    
    def _get_transformation(
        self,
        mapping: MappingInfo,
        instance: InstanceInfo,
    ) -> Optional[TransformationInfo]:
        """Get the transformation definition for an instance."""
        # Check mapping-level transformations first
        if instance.transformation_name in mapping.transformations:
            return mapping.transformations[instance.transformation_name]
            
        # Check folder-level reusable transformations
        if instance.transformation_name in self.folder.transformations:
            return self.folder.transformations[instance.transformation_name]
            
        return None
    
    def build_column_lineage_edges(
        self,
        mapping: MappingInfo,
        session: Optional[SessionInfo] = None,
    ) -> List[ColumnLineageEdge]:
        """Build column-level lineage edges for a mapping."""
        edges = []
        lineage_chains = self.build_mapping_lineage(mapping)
        
        for target_field_key, chain in lineage_chains.items():
            target_table, target_col = target_field_key.split(".", 1)
            target_def = self.folder.targets.get(target_table)
            
            if not target_def:
                continue
                
            # Process each step in the chain
            for step in chain:
                instance = mapping.instances.get(step.instance_name)
                if not instance:
                    continue
                    
                trans = self._get_transformation(mapping, instance)
                
                # Handle Source Qualifier with SQL override
                if (instance.transformation_type == TransformationType.SOURCE_QUALIFIER 
                    and trans and trans.sql_query):
                    sql_edges = self._parse_sql_lineage(
                        trans.sql_query, target_table, target_col, step
                    )
                    edges.extend(sql_edges)
                    
                # Handle Stored Procedure calls
                elif instance.transformation_type == TransformationType.STORED_PROCEDURE:
                    proc_edges = self._build_stored_proc_lineage(
                        trans, target_table, target_col, step
                    )
                    edges.extend(proc_edges)
                    
                # Handle Source Definition (actual source tables)
                elif instance.transformation_type == TransformationType.SOURCE_DEFINITION:
                    source_def = self.folder.sources.get(instance.transformation_name)
                    if source_def:
                        for from_inst, from_field in step.input_fields:
                            edges.append(ColumnLineageEdge(
                                source_dataset=self._build_source_dataset_name(source_def),
                                source_column=from_field,
                                target_dataset=self._build_target_dataset_name(target_def),
                                target_column=target_col,
                                transformation_type=step.transformation_type,
                                transformation_expression=step.expression,
                                step_order=step.step_order,
                            ))
                            
                # Handle standard transformations
                else:
                    for from_inst, from_field in step.input_fields:
                        # Trace back to find the source
                        source_info = self._find_source_for_instance(
                            mapping, from_inst, from_field
                        )
                        if source_info:
                            source_dataset, source_col = source_info
                            edges.append(ColumnLineageEdge(
                                source_dataset=source_dataset,
                                source_column=source_col,
                                target_dataset=self._build_target_dataset_name(target_def),
                                target_column=target_col,
                                transformation_type=step.transformation_type,
                                transformation_expression=step.expression,
                                step_order=step.step_order,
                            ))
                            
        return edges
    
    def _parse_sql_lineage(
        self,
        sql: str,
        target_table: str,
        target_col: str,
        step: TransformationStep,
    ) -> List[ColumnLineageEdge]:
        """Parse SQL query for column lineage."""
        edges = []
        
        # Check for stored procedure call in SQL
        proc_call = self.sql_parser.detect_stored_procedure_call(sql)
        if proc_call:
            schema, proc_name = proc_call
            edges.append(ColumnLineageEdge(
                source_dataset=f"{schema}.{proc_name}" if schema else proc_name,
                source_column="*",  # Unknown column from procedure
                target_dataset=target_table,
                target_column=target_col,
                transformation_type=TransformationType.STORED_PROCEDURE,
                transformation_expression=f"CALL {schema}.{proc_name}" if schema else f"CALL {proc_name}",
                step_order=step.step_order,
            ))
            return edges
            
        # Build source table map for SQL parser
        source_tables = {}
        for source_name, source_def in self.folder.sources.items():
            source_tables[source_name] = source_def.fields
            
        # Parse SQL for column lineage
        try:
            lineage_results = self.sql_parser.parse_sql(sql, source_tables)
            
            for upstream_cols, output_col, expr in lineage_results:
                # Only process if this relates to our target column
                if output_col.upper() == target_col.upper():
                    for table, col in upstream_cols:
                        source_def = self.folder.sources.get(table)
                        if source_def:
                            edges.append(ColumnLineageEdge(
                                source_dataset=self._build_source_dataset_name(source_def),
                                source_column=col,
                                target_dataset=target_table,
                                target_column=target_col,
                                transformation_type=TransformationType.SOURCE_QUALIFIER,
                                transformation_expression=expr,
                                step_order=step.step_order,
                                confidence_score=0.95,
                            ))
        except Exception as e:
            logger.error(f"SQL lineage parsing failed: {e}")
            
        return edges
    
    def _build_stored_proc_lineage(
        self,
        trans: Optional[TransformationInfo],
        target_table: str,
        target_col: str,
        step: TransformationStep,
    ) -> List[ColumnLineageEdge]:
        """Build lineage edge for stored procedure call."""
        edges = []
        
        if trans and trans.stored_proc_name:
            schema = trans.stored_proc_schema or ""
            proc_name = trans.stored_proc_name
            
            edges.append(ColumnLineageEdge(
                source_dataset=f"{schema}.{proc_name}" if schema else proc_name,
                source_column="*",
                target_dataset=target_table,
                target_column=target_col,
                transformation_type=TransformationType.STORED_PROCEDURE,
                transformation_expression=f"EXEC {schema}.{proc_name}" if schema else f"EXEC {proc_name}",
                step_order=step.step_order,
            ))
            
        return edges
    
    def _find_source_for_instance(
        self,
        mapping: MappingInfo,
        instance_name: str,
        field_name: str,
    ) -> Optional[Tuple[str, str]]:
        """Trace back from an instance to find the actual source table and column."""
        visited = set()
        queue = [(instance_name, field_name)]
        
        while queue:
            current_inst, current_field = queue.pop(0)
            visit_key = f"{current_inst}.{current_field}"
            
            if visit_key in visited:
                continue
            visited.add(visit_key)
            
            instance = mapping.instances.get(current_inst)
            if not instance:
                continue
                
            # Found a source definition
            if instance.transformation_type == TransformationType.SOURCE_DEFINITION:
                source_def = self.folder.sources.get(instance.transformation_name)
                if source_def:
                    return (self._build_source_dataset_name(source_def), current_field)
                    
            # Continue tracing upstream via connectors
            for connector in mapping.connectors:
                if (connector.to_instance == current_inst and 
                    connector.to_field == current_field):
                    queue.append((connector.from_instance, connector.from_field))
                    
        return None
    
    def _build_source_dataset_name(self, source: SourceDefinition) -> str:
        """Build dataset name for a source definition."""
        parts = []
        if source.owner_name:
            parts.append(source.owner_name)
        parts.append(source.name)
        return ".".join(parts)
    
    def _build_target_dataset_name(self, target: TargetDefinition) -> str:
        """Build dataset name for a target definition."""
        parts = []
        if target.owner_name:
            parts.append(target.owner_name)
        parts.append(target.name)
        return ".".join(parts)


# =============================================================================
# DataHub Emitter
# =============================================================================

class DataHubLineageEmitter:
    """Emits Informatica lineage to DataHub."""
    
    def __init__(
        self,
        datahub_server: str,
        token: Optional[str] = None,
        platform: str = "oracle",
        platform_instance: Optional[str] = None,
        oracle_platform_instance: Optional[str] = None,
        env: str = "PROD",
        informatica_platform: str = "informatica",
    ):
        self.emitter = DatahubRestEmitter(
            gms_server=datahub_server,
            token=token,
        )
        self.platform = platform
        self.platform_instance = platform_instance
        self.oracle_platform_instance = oracle_platform_instance or platform_instance
        self.env = env
        self.informatica_platform = informatica_platform
        self.mcps: List[MetadataChangeProposalWrapper] = []
        
    def emit_workflow(
        self,
        folder: FolderInfo,
        workflow: WorkflowInfo,
    ) -> str:
        """Emit DataFlow for an Informatica workflow."""
        flow_id = f"{folder.name}/{workflow.name}"
        flow_urn = make_data_flow_urn(
            orchestrator=self.informatica_platform,
            flow_id=flow_id,
            cluster=self.env,
        )
        
        flow_info = DataFlowInfo(
            name=workflow.name,
            description=workflow.description,
            customProperties={
                "folder": folder.name,
                "is_enabled": str(workflow.is_enabled),
                "tool": "PowerCenter",
            },
        )
        
        mcp = MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=flow_info,
        )
        self.mcps.append(mcp)
        
        return flow_urn
    
    def emit_session_as_datajob(
        self,
        folder: FolderInfo,
        workflow: WorkflowInfo,
        session: SessionInfo,
        flow_urn: str,
        lineage_edges: List[ColumnLineageEdge],
        stored_proc_refs: List[str],
    ) -> str:
        """Emit DataJob for an Informatica session with lineage."""
        job_urn = make_data_job_urn_with_flow(
            flow_urn=flow_urn,
            job_id=session.name,
        )
        
        # Create DataJobInfo
        job_info = DataJobInfo(
            name=session.name,
            description=session.description or f"Session for mapping {session.mapping_name}",
            type="INFORMATICA_SESSION",
            customProperties={
                "mapping_name": session.mapping_name,
                "folder": folder.name,
                "is_valid": str(session.is_valid),
            },
        )
        
        mcp_info = MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=job_info,
        )
        self.mcps.append(mcp_info)
        
        # Build input/output datasets and lineage
        input_datasets, output_datasets, fine_grained = self._build_job_io(
            folder, lineage_edges, stored_proc_refs
        )
        
        job_io = DataJobInputOutput(
            inputDatasets=list(input_datasets),
            outputDatasets=list(output_datasets),
            inputDatajobs=stored_proc_refs,  # Reference Oracle stored procs as DataJobs
            fineGrainedLineages=fine_grained if fine_grained else None,
        )
        
        mcp_io = MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=job_io,
        )
        self.mcps.append(mcp_io)
        
        return job_urn
    
    def emit_transformation_datajobs(
        self,
        folder: FolderInfo,
        mapping: MappingInfo,
        session: SessionInfo,
        flow_urn: str,
    ) -> Dict[str, str]:
        """
        Emit individual DataJobs for each transformation in order.
        
        Returns:
            Dict mapping instance names to their DataJob URNs
        """
        job_urns: Dict[str, str] = {}
        
        # Get transformation order from connectors
        ordered_instances = self._get_transformation_order(mapping)
        
        for order, instance_name in enumerate(ordered_instances):
            instance = mapping.instances.get(instance_name)
            if not instance:
                continue
                
            # Skip source and target definitions - they're datasets, not jobs
            if instance.transformation_type in (
                TransformationType.SOURCE_DEFINITION,
                TransformationType.TARGET_DEFINITION,
            ):
                continue
                
            job_id = f"{session.name}/{instance_name}"
            job_urn = make_data_job_urn_with_flow(
                flow_urn=flow_urn,
                job_id=job_id,
            )
            job_urns[instance_name] = job_urn
            
            # Get transformation details
            trans = self._get_transformation(folder, mapping, instance)
            
            job_info = DataJobInfo(
                name=instance_name,
                description=f"{instance.transformation_type.value} transformation",
                type=f"INFORMATICA_{instance.transformation_type.name}",
                customProperties={
                    "transformation_type": instance.transformation_type.value,
                    "transformation_name": instance.transformation_name,
                    "step_order": str(order),
                },
            )
            
            mcp = MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=job_info,
            )
            self.mcps.append(mcp)
            
        return job_urns
    
    def _build_job_io(
        self,
        folder: FolderInfo,
        lineage_edges: List[ColumnLineageEdge],
        stored_proc_refs: List[str],
    ) -> Tuple[Set[str], Set[str], List[FineGrainedLineage]]:
        """Build input/output datasets and fine-grained lineage from edges."""
        input_datasets: Set[str] = set()
        output_datasets: Set[str] = set()
        fine_grained: List[FineGrainedLineage] = []
        
        # Group edges by target column
        edges_by_target: Dict[str, List[ColumnLineageEdge]] = {}
        for edge in lineage_edges:
            target_key = f"{edge.target_dataset}.{edge.target_column}"
            if target_key not in edges_by_target:
                edges_by_target[target_key] = []
            edges_by_target[target_key].append(edge)
            
        for target_key, edges in edges_by_target.items():
            # Get unique upstream fields
            upstream_field_urns = []
            for edge in edges:
                # Build source dataset URN
                source_urn = self._build_dataset_urn(edge.source_dataset, is_source=True)
                input_datasets.add(source_urn)
                
                if edge.source_column != "*":
                    field_urn = make_schema_field_urn(source_urn, edge.source_column)
                    if field_urn not in upstream_field_urns:
                        upstream_field_urns.append(field_urn)
                        
            # Get target dataset URN
            target_parts = target_key.rsplit(".", 1)
            target_dataset = target_parts[0]
            target_column = target_parts[1] if len(target_parts) > 1 else target_parts[0]
            
            target_urn = self._build_dataset_urn(target_dataset, is_source=False)
            output_datasets.add(target_urn)
            target_field_urn = make_schema_field_urn(target_urn, target_column)
            
            # Build transformation expression from edges
            expressions = [e.transformation_expression for e in edges if e.transformation_expression]
            combined_expr = " -> ".join(expressions) if expressions else None
            
            # Get confidence (use minimum from all edges)
            confidence = min(e.confidence_score for e in edges) if edges else 1.0
            
            if upstream_field_urns:
                fgl = FineGrainedLineage(
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    upstreams=upstream_field_urns,
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    downstreams=[target_field_urn],
                    transformOperation=combined_expr,
                    confidenceScore=confidence,
                )
                fine_grained.append(fgl)
                
        return input_datasets, output_datasets, fine_grained
    
    def emit_dataset_lineage(
        self,
        folder: FolderInfo,
        lineage_edges: List[ColumnLineageEdge],
    ):
        """Emit dataset-level UpstreamLineage aspects."""
        # Group by target dataset
        edges_by_target: Dict[str, List[ColumnLineageEdge]] = {}
        for edge in lineage_edges:
            if edge.target_dataset not in edges_by_target:
                edges_by_target[edge.target_dataset] = []
            edges_by_target[edge.target_dataset].append(edge)
            
        for target_dataset, edges in edges_by_target.items():
            target_urn = self._build_dataset_urn(target_dataset, is_source=False)
            
            # Build upstreams
            upstream_datasets: Set[str] = set()
            fine_grained: List[FineGrainedLineage] = []
            
            # Group edges by target column
            edges_by_col: Dict[str, List[ColumnLineageEdge]] = {}
            for edge in edges:
                if edge.target_column not in edges_by_col:
                    edges_by_col[edge.target_column] = []
                edges_by_col[edge.target_column].append(edge)
                
            for target_col, col_edges in edges_by_col.items():
                upstream_fields = []
                
                for edge in col_edges:
                    source_urn = self._build_dataset_urn(edge.source_dataset, is_source=True)
                    upstream_datasets.add(source_urn)
                    
                    if edge.source_column != "*":
                        field_urn = make_schema_field_urn(source_urn, edge.source_column)
                        if field_urn not in upstream_fields:
                            upstream_fields.append(field_urn)
                            
                if upstream_fields:
                    # Build combined expression showing transformation chain
                    sorted_edges = sorted(col_edges, key=lambda e: e.step_order, reverse=True)
                    expr_parts = []
                    for e in sorted_edges:
                        if e.transformation_expression:
                            expr_parts.append(f"[{e.transformation_type.value}] {e.transformation_expression}")
                            
                    combined_expr = " → ".join(expr_parts) if expr_parts else None
                    
                    target_field_urn = make_schema_field_urn(target_urn, target_col)
                    
                    fgl = FineGrainedLineage(
                        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                        upstreams=upstream_fields,
                        downstreamType=FineGrainedLineageDownstreamType.FIELD,
                        downstreams=[target_field_urn],
                        transformOperation=combined_expr,
                        confidenceScore=min(e.confidence_score for e in col_edges),
                    )
                    fine_grained.append(fgl)
                    
            # Build upstream list
            upstreams = [
                Upstream(
                    dataset=ds_urn,
                    type=DatasetLineageType.TRANSFORMED,
                )
                for ds_urn in upstream_datasets
            ]
            
            if upstreams:
                upstream_lineage = UpstreamLineage(
                    upstreams=upstreams,
                    fineGrainedLineages=fine_grained if fine_grained else None,
                )
                
                mcp = MetadataChangeProposalWrapper(
                    entityUrn=target_urn,
                    aspect=upstream_lineage,
                )
                self.mcps.append(mcp)
    
    def build_stored_procedure_refs(
        self,
        lineage_edges: List[ColumnLineageEdge],
    ) -> List[str]:
        """
        Build DataJob URN references for Oracle stored procedures.
        
        These reference procedures that should be ingested separately
        via Oracle ingestion.
        """
        proc_refs: Set[str] = set()
        
        for edge in lineage_edges:
            if edge.transformation_type == TransformationType.STORED_PROCEDURE:
                # Build Oracle stored procedure URN
                # Format: urn:li:dataJob:(urn:li:dataFlow:(oracle,stored_procedures,ENV),PROC_NAME)
                proc_name = edge.source_dataset
                
                # Build as DataJob URN
                flow_urn = make_data_flow_urn(
                    orchestrator="oracle",
                    flow_id="stored_procedures",
                    cluster=self.env,
                )
                
                job_urn = make_data_job_urn_with_flow(
                    flow_urn=flow_urn,
                    job_id=proc_name,
                )
                proc_refs.add(job_urn)
                
        return list(proc_refs)
    
    def build_stored_procedure_dataset_refs(
        self,
        lineage_edges: List[ColumnLineageEdge],
    ) -> List[str]:
        """
        Build Dataset URN references for Oracle stored procedures.
        
        Alternative approach where procedures are modeled as Datasets
        with subtype "Stored Procedure" (matching Oracle connector behavior).
        """
        proc_refs: Set[str] = set()
        
        for edge in lineage_edges:
            if edge.transformation_type == TransformationType.STORED_PROCEDURE:
                proc_name = edge.source_dataset
                
                # Build as Dataset URN (Oracle connector default)
                proc_urn = make_dataset_urn(
                    platform="oracle",
                    name=proc_name,
                    env=self.env,
                )
                proc_refs.add(proc_urn)
                
        return list(proc_refs)
    
    def _build_dataset_urn(self, dataset_name: str, is_source: bool) -> str:
        """Build a dataset URN."""
        platform_instance = (
            self.oracle_platform_instance if is_source 
            else self.platform_instance
        )
        
        if platform_instance:
            # Include platform instance in URN
            urn = f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{platform_instance}.{dataset_name},{self.env})"
        else:
            urn = make_dataset_urn(
                platform=self.platform,
                name=dataset_name,
                env=self.env,
            )
        return urn
    
    def _get_transformation_order(self, mapping: MappingInfo) -> List[str]:
        """Get instances in topological order based on connectors."""
        # Build adjacency list
        incoming: Dict[str, Set[str]] = {name: set() for name in mapping.instances}
        outgoing: Dict[str, Set[str]] = {name: set() for name in mapping.instances}
        
        for connector in mapping.connectors:
            if connector.from_instance in mapping.instances and connector.to_instance in mapping.instances:
                incoming[connector.to_instance].add(connector.from_instance)
                outgoing[connector.from_instance].add(connector.to_instance)
                
        # Topological sort
        result = []
        no_incoming = [name for name, deps in incoming.items() if not deps]
        
        while no_incoming:
            node = no_incoming.pop(0)
            result.append(node)
            
            for next_node in outgoing.get(node, []):
                incoming[next_node].discard(node)
                if not incoming[next_node]:
                    no_incoming.append(next_node)
                    
        # Add any remaining (circular dependencies)
        for name in mapping.instances:
            if name not in result:
                result.append(name)
                
        return result
    
    def _get_transformation(
        self,
        folder: FolderInfo,
        mapping: MappingInfo,
        instance: InstanceInfo,
    ) -> Optional[TransformationInfo]:
        """Get transformation definition for an instance."""
        if instance.transformation_name in mapping.transformations:
            return mapping.transformations[instance.transformation_name]
        if instance.transformation_name in folder.transformations:
            return folder.transformations[instance.transformation_name]
        return None
    
    def flush(self):
        """Emit all accumulated MCPs to DataHub."""
        for mcp in self.mcps:
            self.emitter.emit(mcp)
        logger.info(f"Emitted {len(self.mcps)} metadata change proposals to DataHub")
        self.mcps = []
        
    def test_connection(self) -> bool:
        """Test connection to DataHub."""
        try:
            self.emitter.test_connection()
            return True
        except Exception as e:
            logger.error(f"DataHub connection test failed: {e}")
            return False


# =============================================================================
# Main Extractor Class
# =============================================================================

class InformaticaLineageExtractor:
    """
    Main class for extracting Informatica PowerCenter lineage and emitting to DataHub.
    
    Handles:
    - Standard transformation types with ordered column-level lineage
    - Source Qualifier SQL overrides with SQL parsing
    - Oracle stored procedure calls as cross-platform references
    """
    
    def __init__(
        self,
        datahub_server: str,
        token: Optional[str] = None,
        platform: str = "oracle",
        platform_instance: Optional[str] = None,
        oracle_platform_instance: Optional[str] = None,
        env: str = "PROD",
        emit_intermediate_jobs: bool = True,
        use_dataset_for_procs: bool = False,
    ):
        """
        Initialize the extractor.
        
        Args:
            datahub_server: DataHub GMS server URL
            token: Optional authentication token
            platform: Database platform (default: oracle)
            platform_instance: Platform instance identifier
            oracle_platform_instance: Separate instance for Oracle procs (if different)
            env: Environment (PROD, DEV, etc.)
            emit_intermediate_jobs: Whether to emit individual transformation DataJobs
            use_dataset_for_procs: If True, reference procs as Datasets; if False, as DataJobs
        """
        self.parser = InformaticaXMLParser()
        self.emitter = DataHubLineageEmitter(
            datahub_server=datahub_server,
            token=token,
            platform=platform,
            platform_instance=platform_instance,
            oracle_platform_instance=oracle_platform_instance,
            env=env,
        )
        self.platform = platform
        self.platform_instance = platform_instance
        self.oracle_platform_instance = oracle_platform_instance
        self.env = env
        self.emit_intermediate_jobs = emit_intermediate_jobs
        self.use_dataset_for_procs = use_dataset_for_procs
        
        self.folders: Dict[str, FolderInfo] = {}
        self.all_lineage_edges: List[ColumnLineageEdge] = []
        
    def process_xml_file(self, xml_path: Union[str, Path]) -> Dict[str, FolderInfo]:
        """
        Process an Informatica XML export file.
        
        Args:
            xml_path: Path to the XML file
            
        Returns:
            Dict of parsed folders
        """
        logger.info(f"Processing XML file: {xml_path}")
        self.folders = self.parser.parse_file(xml_path)
        
        for folder_name, folder in self.folders.items():
            logger.info(f"Processing folder: {folder_name}")
            self._process_folder(folder)
            
        return self.folders
    
    def process_xml_string(self, xml_content: str) -> Dict[str, FolderInfo]:
        """
        Process Informatica XML from a string.
        
        Args:
            xml_content: XML content as string
            
        Returns:
            Dict of parsed folders
        """
        self.folders = self.parser.parse_string(xml_content)
        
        for folder_name, folder in self.folders.items():
            logger.info(f"Processing folder: {folder_name}")
            self._process_folder(folder)
            
        return self.folders
    
    def _process_folder(self, folder: FolderInfo):
        """Process all workflows in a folder."""
        lineage_builder = LineageBuilder(
            folder=folder,
            platform=self.platform,
            platform_instance=self.platform_instance,
            env=self.env,
            oracle_platform_instance=self.oracle_platform_instance,
        )
        
        for workflow_name, workflow in folder.workflows.items():
            logger.info(f"Processing workflow: {workflow_name}")
            
            # Emit workflow as DataFlow
            flow_urn = self.emitter.emit_workflow(folder, workflow)
            
            for session_name in workflow.sessions:
                session = folder.sessions.get(session_name)
                if not session:
                    logger.warning(f"Session {session_name} not found in folder")
                    continue
                    
                mapping = folder.mappings.get(session.mapping_name)
                if not mapping:
                    logger.warning(f"Mapping {session.mapping_name} not found for session {session_name}")
                    continue
                    
                logger.info(f"Processing session: {session_name} (mapping: {session.mapping_name})")
                
                # Build column-level lineage edges
                lineage_edges = lineage_builder.build_column_lineage_edges(mapping, session)
                self.all_lineage_edges.extend(lineage_edges)
                
                # Get stored procedure references
                if self.use_dataset_for_procs:
                    proc_refs = self.emitter.build_stored_procedure_dataset_refs(lineage_edges)
                else:
                    proc_refs = self.emitter.build_stored_procedure_refs(lineage_edges)
                    
                # Emit session as main DataJob
                self.emitter.emit_session_as_datajob(
                    folder, workflow, session, flow_urn, lineage_edges, proc_refs
                )
                
                # Optionally emit individual transformation DataJobs
                if self.emit_intermediate_jobs:
                    self.emitter.emit_transformation_datajobs(
                        folder, mapping, session, flow_urn
                    )
                    
        # Emit dataset-level lineage
        if self.all_lineage_edges:
            self.emitter.emit_dataset_lineage(folder, self.all_lineage_edges)
    
    def emit_to_datahub(self) -> bool:
        """
        Emit all collected lineage to DataHub.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.emitter.test_connection():
                logger.error("Failed to connect to DataHub")
                return False
                
            self.emitter.flush()
            logger.info("Successfully emitted lineage to DataHub")
            return True
            
        except Exception as e:
            logger.error(f"Failed to emit to DataHub: {e}")
            return False
    
    def get_lineage_edges(self) -> List[ColumnLineageEdge]:
        """Get all extracted lineage edges."""
        return self.all_lineage_edges
    
    def get_lineage_summary(self) -> Dict[str, Any]:
        """Get summary statistics of extracted lineage."""
        edges_by_type: Dict[str, int] = {}
        source_datasets: Set[str] = set()
        target_datasets: Set[str] = set()
        proc_calls: Set[str] = set()
        
        for edge in self.all_lineage_edges:
            type_name = edge.transformation_type.value
            edges_by_type[type_name] = edges_by_type.get(type_name, 0) + 1
            source_datasets.add(edge.source_dataset)
            target_datasets.add(edge.target_dataset)
            
            if edge.transformation_type == TransformationType.STORED_PROCEDURE:
                proc_calls.add(edge.source_dataset)
                
        return {
            "total_edges": len(self.all_lineage_edges),
            "edges_by_transformation_type": edges_by_type,
            "source_datasets": len(source_datasets),
            "target_datasets": len(target_datasets),
            "stored_procedure_calls": list(proc_calls),
            "folders_processed": len(self.folders),
            "workflows_processed": sum(len(f.workflows) for f in self.folders.values()),
            "mappings_processed": sum(len(f.mappings) for f in self.folders.values()),
        }


# =============================================================================
# CLI Interface
# =============================================================================

def main():
    """Command-line interface for the Informatica lineage extractor."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Extract Informatica PowerCenter lineage and emit to DataHub"
    )
    parser.add_argument(
        "xml_file",
        help="Path to Informatica XML export file",
    )
    parser.add_argument(
        "--datahub-server",
        default="http://localhost:8080",
        help="DataHub GMS server URL",
    )
    parser.add_argument(
        "--token",
        help="DataHub authentication token",
    )
    parser.add_argument(
        "--platform",
        default="oracle",
        help="Database platform (default: oracle)",
    )
    parser.add_argument(
        "--platform-instance",
        help="Platform instance identifier",
    )
    parser.add_argument(
        "--oracle-platform-instance",
        help="Separate Oracle platform instance for stored procedure references",
    )
    parser.add_argument(
        "--env",
        default="PROD",
        help="Environment (default: PROD)",
    )
    parser.add_argument(
        "--emit-intermediate-jobs",
        action="store_true",
        default=True,
        help="Emit individual transformation DataJobs",
    )
    parser.add_argument(
        "--use-dataset-for-procs",
        action="store_true",
        help="Reference stored procedures as Datasets instead of DataJobs",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and analyze without emitting to DataHub",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        
    extractor = InformaticaLineageExtractor(
        datahub_server=args.datahub_server,
        token=args.token,
        platform=args.platform,
        platform_instance=args.platform_instance,
        oracle_platform_instance=args.oracle_platform_instance,
        env=args.env,
        emit_intermediate_jobs=args.emit_intermediate_jobs,
        use_dataset_for_procs=args.use_dataset_for_procs,
    )
    
    # Process XML
    extractor.process_xml_file(args.xml_file)
    
    # Print summary
    summary = extractor.get_lineage_summary()
    print("\n" + "=" * 60)
    print("Informatica Lineage Extraction Summary")
    print("=" * 60)
    print(f"Total lineage edges: {summary['total_edges']}")
    print(f"Source datasets: {summary['source_datasets']}")
    print(f"Target datasets: {summary['target_datasets']}")
    print(f"Workflows processed: {summary['workflows_processed']}")
    print(f"Mappings processed: {summary['mappings_processed']}")
    
    if summary['stored_procedure_calls']:
        print(f"\nStored procedure references:")
        for proc in summary['stored_procedure_calls']:
            print(f"  - {proc}")
            
    print(f"\nEdges by transformation type:")
    for trans_type, count in summary['edges_by_transformation_type'].items():
        print(f"  {trans_type}: {count}")
        
    # Emit to DataHub unless dry run
    if not args.dry_run:
        success = extractor.emit_to_datahub()
        if success:
            print("\n✓ Successfully emitted lineage to DataHub")
        else:
            print("\n✗ Failed to emit lineage to DataHub")
            return 1
    else:
        print("\n[Dry run - no data emitted to DataHub]")
        
    return 0


if __name__ == "__main__":
    exit(main())
