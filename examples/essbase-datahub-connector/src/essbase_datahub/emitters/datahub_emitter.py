"""
DataHub emitter for Essbase metadata.

Converts OpenLineage-compatible intermediate format to DataHub entities
and emits using the DataHub Python SDK.
"""

from __future__ import annotations
import logging
from typing import Optional, Any
from datetime import datetime

from datahub.emitter.mce_builder import (
    make_dataset_urn,
    make_data_job_urn,
    make_data_flow_urn,
    make_tag_urn,
    make_term_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    # Dataset aspects
    DatasetPropertiesClass,
    SchemaMetadataClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    NumberTypeClass,
    DateTypeClass,
    ArrayTypeClass,
    MapTypeClass,
    GlobalTagsClass,
    TagAssociationClass,
    GlossaryTermsClass,
    GlossaryTermAssociationClass,
    OwnershipClass,
    OwnerClass,
    OwnershipTypeClass,
    AuditStampClass,
    # Lineage aspects
    UpstreamLineageClass,
    UpstreamClass,
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageUpstreamTypeClass,
    FineGrainedLineageDownstreamTypeClass,
    # DataJob aspects
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataFlowInfoClass,
    # Custom properties
    DatasetKeyClass,
    StatusClass,
)

from ..models.openlineage_models import (
    EssbaseMetadataExtraction,
    OpenLineageDataset,
    OpenLineageJob,
    ColumnLineageMapping,
    SchemaField,
    SchemaFieldType,
)

logger = logging.getLogger(__name__)


class DataHubEmitter:
    """
    Emits Essbase metadata to DataHub.
    
    Converts OpenLineage-compatible models to DataHub MCPs and
    sends them via the REST emitter.
    """
    
    # Mapping of SchemaFieldType to DataHub types
    TYPE_MAPPING = {
        SchemaFieldType.STRING: StringTypeClass,
        SchemaFieldType.NUMBER: NumberTypeClass,
        SchemaFieldType.INTEGER: NumberTypeClass,
        SchemaFieldType.DOUBLE: NumberTypeClass,
        SchemaFieldType.BOOLEAN: StringTypeClass,  # DataHub doesn't have a bool type
        SchemaFieldType.DATE: DateTypeClass,
        SchemaFieldType.TIMESTAMP: DateTypeClass,
        SchemaFieldType.ARRAY: ArrayTypeClass,
        SchemaFieldType.STRUCT: MapTypeClass,
        SchemaFieldType.MEMBER: StringTypeClass,
        SchemaFieldType.DIMENSION: StringTypeClass,
        SchemaFieldType.MEASURE: NumberTypeClass,
    }
    
    def __init__(
        self,
        datahub_server: str,
        token: Optional[str] = None,
        platform: str = "essbase",
        platform_instance: Optional[str] = None,
        env: str = "PROD",
        dry_run: bool = False,
    ):
        self.platform = platform
        self.platform_instance = platform_instance
        self.env = env
        self.dry_run = dry_run
        
        if not dry_run:
            self.emitter = DatahubRestEmitter(
                gms_server=datahub_server,
                token=token,
            )
        else:
            self.emitter = None
        
        self._emitted_count = 0
        self._error_count = 0
    
    # =========================================================================
    # Main Emission Methods
    # =========================================================================
    
    def emit_extraction(self, extraction: EssbaseMetadataExtraction) -> dict[str, int]:
        """
        Emit a complete metadata extraction to DataHub.
        
        Returns dict with counts of emitted entities by type.
        """
        counts = {
            "datasets": 0,
            "jobs": 0,
            "lineage_edges": 0,
            "errors": 0,
        }
        
        # Emit datasets
        for dataset in extraction.datasets:
            try:
                self.emit_dataset(dataset)
                counts["datasets"] += 1
            except Exception as e:
                logger.error(f"Error emitting dataset {dataset.name}: {e}")
                counts["errors"] += 1
        
        # Emit data flows and jobs
        flows_emitted = set()
        for job in extraction.jobs:
            try:
                # Emit flow if not already emitted
                flow_urn = job.flow_urn or job.namespace
                if flow_urn not in flows_emitted:
                    self._emit_data_flow(flow_urn, job.namespace)
                    flows_emitted.add(flow_urn)
                
                self.emit_job(job)
                counts["jobs"] += 1
            except Exception as e:
                logger.error(f"Error emitting job {job.name}: {e}")
                counts["errors"] += 1
        
        # Emit standalone lineage edges
        for lineage in extraction.lineage_edges:
            try:
                self._emit_column_lineage(lineage)
                counts["lineage_edges"] += 1
            except Exception as e:
                logger.error(f"Error emitting lineage: {e}")
                counts["errors"] += 1
        
        logger.info(f"Emission complete: {counts}")
        return counts
    
    def emit_dataset(self, dataset: OpenLineageDataset) -> str:
        """Emit a single dataset with schema and properties."""
        dataset_urn = self._make_dataset_urn(dataset.name)
        
        mcps = []
        
        # Dataset properties
        props = DatasetPropertiesClass(
            name=dataset.name,
            description=dataset.facets.documentation.get("description") if dataset.facets.documentation else None,
            customProperties=dataset.custom_properties,
            tags=dataset.tags,
        )
        mcps.append(MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=props,
        ))
        
        # Schema
        if dataset.schema_fields:
            schema = self._build_schema_metadata(dataset)
            mcps.append(MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=schema,
            ))
        
        # Tags
        if dataset.tags:
            tags = GlobalTagsClass(
                tags=[TagAssociationClass(tag=make_tag_urn(t)) for t in dataset.tags]
            )
            mcps.append(MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=tags,
            ))
        
        # Glossary terms
        if dataset.glossary_terms:
            terms = GlossaryTermsClass(
                terms=[
                    GlossaryTermAssociationClass(urn=make_term_urn(t))
                    for t in dataset.glossary_terms
                ],
                auditStamp=AuditStampClass(
                    time=int(datetime.now().timestamp() * 1000),
                    actor="urn:li:corpuser:datahub",
                ),
            )
            mcps.append(MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=terms,
            ))
        
        # Status
        mcps.append(MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=StatusClass(removed=False),
        ))
        
        # Emit all MCPs
        self._emit_mcps(mcps)
        
        logger.info(f"Emitted dataset: {dataset_urn}")
        return dataset_urn
    
    def emit_job(self, job: OpenLineageJob) -> str:
        """Emit a data job with lineage."""
        flow_urn = make_data_flow_urn(
            orchestrator=self.platform,
            flow_id=job.flow_urn or job.namespace,
            env=self.env,
        )
        
        job_urn = make_data_job_urn(
            orchestrator=self.platform,
            flow_id=job.flow_urn or job.namespace,
            job_id=job.name,
            env=self.env,
        )
        
        mcps = []
        
        # Job info
        job_info = DataJobInfoClass(
            name=job.name,
            type=job.job_type,
            description=job.description,
            customProperties=job.custom_properties,
        )
        mcps.append(MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=job_info,
        ))
        
        # Input/Output
        input_urns = [
            self._make_dataset_urn(inp.name)
            for inp in job.inputs
        ]
        output_urns = [
            self._make_dataset_urn(out.name)
            for out in job.outputs
        ]
        
        io_aspect = DataJobInputOutputClass(
            inputDatasets=input_urns,
            outputDatasets=output_urns,
            inputDatajobs=[],
        )
        mcps.append(MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=io_aspect,
        ))
        
        # Tags
        if job.tags:
            tags = GlobalTagsClass(
                tags=[TagAssociationClass(tag=make_tag_urn(t)) for t in job.tags]
            )
            mcps.append(MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=tags,
            ))
        
        # Status
        mcps.append(MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=StatusClass(removed=False),
        ))
        
        self._emit_mcps(mcps)
        
        # Emit fine-grained lineage separately
        if job.column_lineage:
            self._emit_job_column_lineage(job, output_urns)
        
        logger.info(f"Emitted job: {job_urn}")
        return job_urn
    
    # =========================================================================
    # Schema Building
    # =========================================================================
    
    def _build_schema_metadata(self, dataset: OpenLineageDataset) -> SchemaMetadataClass:
        """Build DataHub schema from OpenLineage schema fields."""
        fields = []
        
        for field in dataset.schema_fields:
            dh_fields = self._convert_schema_field(field, "")
            fields.extend(dh_fields)
        
        return SchemaMetadataClass(
            schemaName=f"{dataset.name}_schema",
            platform=f"urn:li:dataPlatform:{self.platform}",
            version=0,
            hash="",
            platformSchema=SchemaMetadataClass.PlatformSchema(
                esSchema=None,
            ),
            fields=fields,
        )
    
    def _convert_schema_field(
        self,
        field: SchemaField,
        parent_path: str,
    ) -> list[SchemaFieldClass]:
        """Convert OpenLineage field to DataHub SchemaFieldClass(es)."""
        results = []
        
        field_path = f"{parent_path}.{field.name}" if parent_path else field.name
        
        # Get type class
        type_class = self.TYPE_MAPPING.get(field.type, StringTypeClass)()
        
        # Build custom properties from Essbase metadata
        custom_props = dict(field.custom_properties)
        if field.essbase_metadata:
            meta = field.essbase_metadata
            if meta.formula:
                custom_props["essbase_formula"] = meta.formula
            if meta.consolidation:
                custom_props["essbase_consolidation"] = meta.consolidation
            if meta.storage_type:
                custom_props["essbase_storage"] = meta.storage_type
            if meta.level is not None:
                custom_props["essbase_level"] = str(meta.level)
            if meta.generation is not None:
                custom_props["essbase_generation"] = str(meta.generation)
            if meta.uda:
                custom_props["essbase_uda"] = ",".join(meta.uda)
        
        dh_field = SchemaFieldClass(
            fieldPath=field_path,
            type=SchemaFieldDataTypeClass(type=type_class),
            nativeDataType=field.native_type or field.type.value,
            description=field.description,
            globalTags=GlobalTagsClass(
                tags=[TagAssociationClass(tag=make_tag_urn(t)) for t in field.tags]
            ) if field.tags else None,
            glossaryTerms=GlossaryTermsClass(
                terms=[GlossaryTermAssociationClass(urn=make_term_urn(t)) for t in field.glossary_terms],
                auditStamp=AuditStampClass(
                    time=int(datetime.now().timestamp() * 1000),
                    actor="urn:li:corpuser:datahub",
                ),
            ) if field.glossary_terms else None,
        )
        
        results.append(dh_field)
        
        # Process nested fields (for hierarchical members)
        for nested in field.fields:
            nested_results = self._convert_schema_field(nested, field_path)
            results.extend(nested_results)
        
        return results
    
    # =========================================================================
    # Lineage Emission
    # =========================================================================
    
    def _emit_job_column_lineage(
        self,
        job: OpenLineageJob,
        output_urns: list[str],
    ) -> None:
        """Emit fine-grained column lineage for a job."""
        if not job.column_lineage or not output_urns:
            return
        
        # Group lineage by target dataset
        lineage_by_target: dict[str, list[ColumnLineageMapping]] = {}
        for mapping in job.column_lineage:
            target_urn = self._make_dataset_urn(mapping.target_dataset)
            if target_urn not in lineage_by_target:
                lineage_by_target[target_urn] = []
            lineage_by_target[target_urn].append(mapping)
        
        for target_urn, mappings in lineage_by_target.items():
            fine_grained = []
            upstream_datasets = set()
            
            for mapping in mappings:
                source_urn = self._make_dataset_urn(mapping.source_dataset)
                upstream_datasets.add(source_urn)
                
                fg = FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    upstreams=[f"{source_urn}/{mapping.source_field}"],
                    downstreams=[f"{target_urn}/{mapping.target_field}"],
                    transformOperation=mapping.transformation_type,
                )
                fine_grained.append(fg)
            
            upstreams = [
                UpstreamClass(
                    dataset=ds_urn,
                    type=DatasetLineageTypeClass.TRANSFORMED,
                )
                for ds_urn in upstream_datasets
            ]
            
            lineage_aspect = UpstreamLineageClass(
                upstreams=upstreams,
                fineGrainedLineages=fine_grained,
            )
            
            mcp = MetadataChangeProposalWrapper(
                entityUrn=target_urn,
                aspect=lineage_aspect,
            )
            
            self._emit_mcps([mcp])
    
    def _emit_column_lineage(self, mapping: ColumnLineageMapping) -> None:
        """Emit a standalone column-level lineage edge."""
        target_urn = self._make_dataset_urn(mapping.target_dataset)
        source_urn = self._make_dataset_urn(mapping.source_dataset)
        
        fg = FineGrainedLineageClass(
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
            upstreams=[f"{source_urn}/{mapping.source_field}"],
            downstreams=[f"{target_urn}/{mapping.target_field}"],
            transformOperation=mapping.transformation_type,
        )
        
        lineage_aspect = UpstreamLineageClass(
            upstreams=[
                UpstreamClass(
                    dataset=source_urn,
                    type=DatasetLineageTypeClass.TRANSFORMED,
                )
            ],
            fineGrainedLineages=[fg],
        )
        
        mcp = MetadataChangeProposalWrapper(
            entityUrn=target_urn,
            aspect=lineage_aspect,
        )
        
        self._emit_mcps([mcp])
    
    def _emit_data_flow(self, flow_id: str, namespace: str) -> str:
        """Emit a data flow (container for jobs)."""
        flow_urn = make_data_flow_urn(
            orchestrator=self.platform,
            flow_id=flow_id,
            env=self.env,
        )
        
        flow_info = DataFlowInfoClass(
            name=flow_id,
            description=f"Essbase application: {namespace}",
            customProperties={
                "platform": "essbase",
                "namespace": namespace,
            },
        )
        
        mcp = MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=flow_info,
        )
        
        self._emit_mcps([mcp])
        
        return flow_urn
    
    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def _make_dataset_urn(self, name: str) -> str:
        """Create a dataset URN."""
        if self.platform_instance:
            return make_dataset_urn(
                platform=self.platform,
                name=name,
                env=self.env,
                platform_instance=self.platform_instance,
            )
        return make_dataset_urn(
            platform=self.platform,
            name=name,
            env=self.env,
        )
    
    def _emit_mcps(self, mcps: list[MetadataChangeProposalWrapper]) -> None:
        """Emit a list of MCPs."""
        for mcp in mcps:
            if self.dry_run:
                logger.debug(f"[DRY RUN] Would emit: {mcp.entityUrn}")
                self._emitted_count += 1
            else:
                try:
                    self.emitter.emit(mcp)
                    self._emitted_count += 1
                except Exception as e:
                    logger.error(f"Error emitting MCP for {mcp.entityUrn}: {e}")
                    self._error_count += 1
    
    def flush(self) -> None:
        """Flush any buffered emissions."""
        if self.emitter and not self.dry_run:
            self.emitter.flush()
    
    def close(self) -> None:
        """Close the emitter connection."""
        self.flush()
    
    @property
    def stats(self) -> dict[str, int]:
        """Get emission statistics."""
        return {
            "emitted": self._emitted_count,
            "errors": self._error_count,
        }
