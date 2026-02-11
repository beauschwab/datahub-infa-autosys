from __future__ import annotations

import time
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    DatasetLineageTypeClass,
    EdgeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    UpstreamClass,
    UpstreamLineageClass,
)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _system_actor() -> str:
    # Pick a stable actor urn; you can override in sink configs if desired.
    return "urn:li:corpuser:datahub"


def _audit_stamp() -> AuditStampClass:
    return AuditStampClass(time=_now_ms(), actor=_system_actor())


def make_edge(destination_urn: str, properties: Optional[Dict[str, str]] = None) -> EdgeClass:
    """
    EdgeClass is the supported object for job->dataset and job->job relationships.
    Signature is stable in the Python SDK models.
    """
    return EdgeClass(destinationUrn=destination_urn, created=_audit_stamp(), properties=properties or None)


def mcp_dataflow_info(flow_urn: str, name: str, description: Optional[str] = None, external_url: Optional[str] = None) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(
        entityUrn=flow_urn,
        aspect=DataFlowInfoClass(
            name=name,
            description=description,
            externalUrl=external_url,
            customProperties=None,
        ),
    )


def mcp_datajob_info(
    job_urn: str,
    name: str,
    job_type: str,
    flow_urn: Optional[str] = None,
    description: Optional[str] = None,
    external_url: Optional[str] = None,
    custom_properties: Optional[Dict[str, str]] = None,
) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(
        entityUrn=job_urn,
        aspect=DataJobInfoClass(
            name=name,
            type=job_type,  # NOTE: docs recommend strings; AzkabanJobType enum is deprecated.
            flowUrn=flow_urn,
            description=description,
            externalUrl=external_url,
            customProperties=custom_properties or None,
            created=None,
            lastModified=None,
            status=None,
            env=None,
        ),
    )


def mcp_datajob_io(
    job_urn: str,
    input_datasets: Sequence[str],
    output_datasets: Sequence[str],
    input_dataset_edges: Optional[Sequence[EdgeClass]] = None,
    output_dataset_edges: Optional[Sequence[EdgeClass]] = None,
    input_datajob_edges: Optional[Sequence[EdgeClass]] = None,
    input_fields: Optional[Sequence[str]] = None,
    output_fields: Optional[Sequence[str]] = None,
) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(
        entityUrn=job_urn,
        aspect=DataJobInputOutputClass(
            inputDatasets=list(input_datasets),
            outputDatasets=list(output_datasets),
            inputDatasetEdges=list(input_dataset_edges) if input_dataset_edges else None,
            outputDatasetEdges=list(output_dataset_edges) if output_dataset_edges else None,
            inputDatajobEdges=list(input_datajob_edges) if input_datajob_edges else None,
            inputDatasetFields=list(input_fields) if input_fields else None,
            outputDatasetFields=list(output_fields) if output_fields else None,
            fineGrainedLineages=None,  # prefer dataset-level UpstreamLineage for UI support
        ),
    )


def mcp_dataset_platform_instance(dataset_urn: str, platform: str, instance: str) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=DataPlatformInstanceClass(platform=f"urn:li:dataPlatform:{platform}", instance=instance),
    )


def mcp_upstream_lineage(
    downstream_dataset_urn: str,
    upstream_dataset_urns: Sequence[str],
    fine_grained_field_mappings: Optional[Sequence[Tuple[str, Sequence[str], Sequence[str], Optional[str]]]] = None,
    lineage_type: str = DatasetLineageTypeClass.TRANSFORMED,
    upstream_properties: Optional[Dict[str, str]] = None,
) -> MetadataChangeProposalWrapper:
    """
    Build dataset-level lineage suitable for DataHub UI:
      - coarse dataset upstreams (UpstreamLineage.upstreams)
      - optional fine-grained lineage using schemaField URNs (UpstreamLineage.fineGrainedLineages)

    fine_grained_field_mappings elements:
      (transformOperation, [upstream_field_paths], [downstream_field_paths], query_text)
    where field paths are column paths within the dataset schema (e.g. "PRODUCT_ID").
    """
    upstreams = [
        UpstreamClass(
            dataset=u,
            type=lineage_type,
            auditStamp=_audit_stamp(),
            properties=upstream_properties or None,
            query=None,
            created=None,
        )
        for u in upstream_dataset_urns
    ]

    fgl: List[FineGrainedLineageClass] = []
    if fine_grained_field_mappings:
        for transform_op, ups, dns, query_text in fine_grained_field_mappings:
            upstream_field_urns: List[str] = []
            downstream_field_urns: List[str] = []
            for up in ups:
                # `ups` may be schemaField URNs already, or raw field paths.
                upstream_field_urns.append(
                    up if up.startswith("urn:li:schemaField:") else make_schema_field_urn(upstream_dataset_urns[0], up)
                )
            for dn in dns:
                downstream_field_urns.append(
                    dn if dn.startswith("urn:li:schemaField:") else make_schema_field_urn(downstream_dataset_urn, dn)
                )

            fgl.append(
                FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    upstreams=upstream_field_urns,
                    downstreams=downstream_field_urns,
                    transformOperation=transform_op,
                    confidenceScore=None,
                    query=query_text,
                )
            )

    return MetadataChangeProposalWrapper(
        entityUrn=downstream_dataset_urn,
        aspect=UpstreamLineageClass(upstreams=upstreams, fineGrainedLineages=fgl or None),
    )


def mcp_dataset_properties(
    dataset_urn: str,
    name: str,
    description: Optional[str] = None,
    custom_properties: Optional[Dict[str, str]] = None,
    tags: Optional[Sequence[str]] = None,
) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=DatasetPropertiesClass(
            name=name,
            description=description,
            customProperties=custom_properties or None,
            tags=list(tags) if tags else None,
        ),
    )


def mcp_schema_metadata(
    dataset_urn: str,
    platform: str,
    fields: Sequence[SchemaFieldClass],
    schema_name: Optional[str] = None,
) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=SchemaMetadataClass(
            schemaName=schema_name or "default",
            platform=f"urn:li:dataPlatform:{platform}",
            version=0,
            hash="",
            platformSchema=SchemaMetadataClass.PlatformSchema(esSchema=None),
            fields=list(fields),
        ),
    )
