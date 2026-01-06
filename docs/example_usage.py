"""
Example usage of the Informatica PowerCenter to DataHub lineage extractor.

This script demonstrates:
1. Processing a sample Informatica XML export
2. Handling Source Qualifier SQL overrides
3. Handling Oracle stored procedure calls
4. Emitting ordered column-level lineage to DataHub
"""

from informatica_lineage import (
    InformaticaLineageExtractor,
    InformaticaXMLParser,
    LineageBuilder,
    TransformationType,
)

# =============================================================================
# Sample Informatica XML with SQL Override and Stored Procedure
# =============================================================================

SAMPLE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE POWERMART SYSTEM "powrmart.dtd">
<POWERMART CREATION_DATE="01/06/2026 10:00:00" REPOSITORY_VERSION="186.95">
<REPOSITORY NAME="DEV_REPO" VERSION="186" CODEPAGE="UTF-8" DATABASETYPE="Oracle">
<FOLDER NAME="CUSTOMER_ETL" OWNER="admin" SHARED="NOTSHARED" DESCRIPTION="Customer ETL Processes" VERSION="1">

<!-- Source Definitions -->
<SOURCE DBDNAME="CRM_DB" DATABASETYPE="Oracle" NAME="CUSTOMERS" OWNERNAME="CRM">
    <SOURCEFIELD NAME="CUSTOMER_ID" DATATYPE="number" PRECISION="10" SCALE="0" NULLABLE="NOTNULL"/>
    <SOURCEFIELD NAME="FIRST_NAME" DATATYPE="string" PRECISION="50" SCALE="0" NULLABLE="NULL"/>
    <SOURCEFIELD NAME="LAST_NAME" DATATYPE="string" PRECISION="50" SCALE="0" NULLABLE="NULL"/>
    <SOURCEFIELD NAME="EMAIL" DATATYPE="string" PRECISION="100" SCALE="0" NULLABLE="NULL"/>
    <SOURCEFIELD NAME="CREATED_DATE" DATATYPE="date/time" PRECISION="19" SCALE="0" NULLABLE="NULL"/>
    <SOURCEFIELD NAME="STATUS_CODE" DATATYPE="string" PRECISION="10" SCALE="0" NULLABLE="NULL"/>
</SOURCE>

<SOURCE DBDNAME="CRM_DB" DATABASETYPE="Oracle" NAME="ORDERS" OWNERNAME="CRM">
    <SOURCEFIELD NAME="ORDER_ID" DATATYPE="number" PRECISION="10" SCALE="0" NULLABLE="NOTNULL"/>
    <SOURCEFIELD NAME="CUSTOMER_ID" DATATYPE="number" PRECISION="10" SCALE="0" NULLABLE="NOTNULL"/>
    <SOURCEFIELD NAME="ORDER_DATE" DATATYPE="date/time" PRECISION="19" SCALE="0" NULLABLE="NULL"/>
    <SOURCEFIELD NAME="TOTAL_AMOUNT" DATATYPE="decimal" PRECISION="15" SCALE="2" NULLABLE="NULL"/>
    <SOURCEFIELD NAME="CURRENCY_CODE" DATATYPE="string" PRECISION="3" SCALE="0" NULLABLE="NULL"/>
</SOURCE>

<!-- Target Definitions -->
<TARGET DATABASETYPE="Oracle" NAME="DIM_CUSTOMERS" OWNERNAME="DW">
    <TARGETFIELD NAME="CUSTOMER_KEY" DATATYPE="number" PRECISION="10" SCALE="0" NULLABLE="NOTNULL"/>
    <TARGETFIELD NAME="CUSTOMER_ID" DATATYPE="number" PRECISION="10" SCALE="0" NULLABLE="NOTNULL"/>
    <TARGETFIELD NAME="FULL_NAME" DATATYPE="string" PRECISION="100" SCALE="0" NULLABLE="NULL"/>
    <TARGETFIELD NAME="EMAIL_DOMAIN" DATATYPE="string" PRECISION="50" SCALE="0" NULLABLE="NULL"/>
    <TARGETFIELD NAME="TOTAL_ORDERS" DATATYPE="number" PRECISION="10" SCALE="0" NULLABLE="NULL"/>
    <TARGETFIELD NAME="LIFETIME_VALUE" DATATYPE="decimal" PRECISION="15" SCALE="2" NULLABLE="NULL"/>
    <TARGETFIELD NAME="CUSTOMER_STATUS" DATATYPE="string" PRECISION="20" SCALE="0" NULLABLE="NULL"/>
    <TARGETFIELD NAME="ETL_TIMESTAMP" DATATYPE="date/time" PRECISION="19" SCALE="0" NULLABLE="NULL"/>
</TARGET>

<!-- Mapping with SQL Override and Stored Procedure -->
<MAPPING NAME="m_customer_dimension" ISVALID="YES" DESCRIPTION="Customer dimension load with enrichment">

<!-- Source Instance -->
<INSTANCE NAME="CUSTOMERS" TRANSFORMATION_NAME="CUSTOMERS" TRANSFORMATION_TYPE="Source Definition" REUSABLE="NO"/>
<INSTANCE NAME="ORDERS" TRANSFORMATION_NAME="ORDERS" TRANSFORMATION_TYPE="Source Definition" REUSABLE="NO"/>

<!-- Source Qualifier with SQL Override (embedded SQL requiring parsing) -->
<INSTANCE NAME="SQ_CUSTOMER_ORDERS" TRANSFORMATION_NAME="SQ_CUSTOMER_ORDERS" TRANSFORMATION_TYPE="Source Qualifier" REUSABLE="NO"/>

<TRANSFORMATION NAME="SQ_CUSTOMER_ORDERS" TYPE="Source Qualifier">
    <TRANSFORMFIELD NAME="CUSTOMER_ID" DATATYPE="number" PRECISION="10" SCALE="0" PORTTYPE="OUTPUT"/>
    <TRANSFORMFIELD NAME="FIRST_NAME" DATATYPE="string" PRECISION="50" SCALE="0" PORTTYPE="OUTPUT"/>
    <TRANSFORMFIELD NAME="LAST_NAME" DATATYPE="string" PRECISION="50" SCALE="0" PORTTYPE="OUTPUT"/>
    <TRANSFORMFIELD NAME="EMAIL" DATATYPE="string" PRECISION="100" SCALE="0" PORTTYPE="OUTPUT"/>
    <TRANSFORMFIELD NAME="STATUS_CODE" DATATYPE="string" PRECISION="10" SCALE="0" PORTTYPE="OUTPUT"/>
    <TRANSFORMFIELD NAME="ORDER_COUNT" DATATYPE="number" PRECISION="10" SCALE="0" PORTTYPE="OUTPUT"/>
    <TRANSFORMFIELD NAME="TOTAL_REVENUE" DATATYPE="decimal" PRECISION="15" SCALE="2" PORTTYPE="OUTPUT"/>
    <TABLEATTRIBUTE NAME="Sql Query" VALUE="SELECT 
        c.CUSTOMER_ID,
        c.FIRST_NAME,
        c.LAST_NAME,
        c.EMAIL,
        c.STATUS_CODE,
        COUNT(o.ORDER_ID) AS ORDER_COUNT,
        SUM(o.TOTAL_AMOUNT) AS TOTAL_REVENUE
    FROM CRM.CUSTOMERS c
    LEFT JOIN CRM.ORDERS o ON c.CUSTOMER_ID = o.CUSTOMER_ID
    WHERE c.STATUS_CODE != 'DELETED'
    GROUP BY c.CUSTOMER_ID, c.FIRST_NAME, c.LAST_NAME, c.EMAIL, c.STATUS_CODE"/>
</TRANSFORMATION>

<!-- Expression Transformation -->
<INSTANCE NAME="EXP_TRANSFORM" TRANSFORMATION_NAME="EXP_TRANSFORM" TRANSFORMATION_TYPE="Expression" REUSABLE="NO"/>

<TRANSFORMATION NAME="EXP_TRANSFORM" TYPE="Expression">
    <TRANSFORMFIELD NAME="CUSTOMER_ID" DATATYPE="number" PRECISION="10" SCALE="0" PORTTYPE="INPUT"/>
    <TRANSFORMFIELD NAME="FIRST_NAME" DATATYPE="string" PRECISION="50" SCALE="0" PORTTYPE="INPUT"/>
    <TRANSFORMFIELD NAME="LAST_NAME" DATATYPE="string" PRECISION="50" SCALE="0" PORTTYPE="INPUT"/>
    <TRANSFORMFIELD NAME="EMAIL" DATATYPE="string" PRECISION="100" SCALE="0" PORTTYPE="INPUT"/>
    <TRANSFORMFIELD NAME="STATUS_CODE" DATATYPE="string" PRECISION="10" SCALE="0" PORTTYPE="INPUT"/>
    <TRANSFORMFIELD NAME="ORDER_COUNT" DATATYPE="number" PRECISION="10" SCALE="0" PORTTYPE="INPUT"/>
    <TRANSFORMFIELD NAME="TOTAL_REVENUE" DATATYPE="decimal" PRECISION="15" SCALE="2" PORTTYPE="INPUT"/>
    <TRANSFORMFIELD NAME="FULL_NAME" DATATYPE="string" PRECISION="100" SCALE="0" PORTTYPE="OUTPUT" EXPRESSION="INITCAP(FIRST_NAME) || ' ' || UPPER(LAST_NAME)"/>
    <TRANSFORMFIELD NAME="EMAIL_DOMAIN" DATATYPE="string" PRECISION="50" SCALE="0" PORTTYPE="OUTPUT" EXPRESSION="SUBSTR(EMAIL, INSTR(EMAIL, '@') + 1)"/>
    <TRANSFORMFIELD NAME="CUSTOMER_STATUS" DATATYPE="string" PRECISION="20" SCALE="0" PORTTYPE="OUTPUT" EXPRESSION="IIF(ORDER_COUNT > 10, 'VIP', IIF(ORDER_COUNT > 0, 'ACTIVE', 'PROSPECT'))"/>
    <TRANSFORMFIELD NAME="o_CUSTOMER_ID" DATATYPE="number" PRECISION="10" SCALE="0" PORTTYPE="OUTPUT" EXPRESSION="CUSTOMER_ID"/>
    <TRANSFORMFIELD NAME="o_ORDER_COUNT" DATATYPE="number" PRECISION="10" SCALE="0" PORTTYPE="OUTPUT" EXPRESSION="ORDER_COUNT"/>
    <TRANSFORMFIELD NAME="o_TOTAL_REVENUE" DATATYPE="decimal" PRECISION="15" SCALE="2" PORTTYPE="OUTPUT" EXPRESSION="TOTAL_REVENUE"/>
</TRANSFORMATION>

<!-- Stored Procedure Call for Customer Key Lookup -->
<INSTANCE NAME="SP_GET_CUSTOMER_KEY" TRANSFORMATION_NAME="SP_GET_CUSTOMER_KEY" TRANSFORMATION_TYPE="Stored Procedure" REUSABLE="NO"/>

<TRANSFORMATION NAME="SP_GET_CUSTOMER_KEY" TYPE="Stored Procedure">
    <TRANSFORMFIELD NAME="in_CUSTOMER_ID" DATATYPE="number" PRECISION="10" SCALE="0" PORTTYPE="INPUT"/>
    <TRANSFORMFIELD NAME="out_CUSTOMER_KEY" DATATYPE="number" PRECISION="10" SCALE="0" PORTTYPE="OUTPUT"/>
    <TABLEATTRIBUTE NAME="Stored Procedure Name" VALUE="PKG_CUSTOMER.GET_OR_CREATE_KEY"/>
    <TABLEATTRIBUTE NAME="Connection Information" VALUE="Oracle:DW_DB"/>
</TRANSFORMATION>

<!-- Another Stored Procedure for Status Enrichment -->
<INSTANCE NAME="SP_ENRICH_STATUS" TRANSFORMATION_NAME="SP_ENRICH_STATUS" TRANSFORMATION_TYPE="Stored Procedure" REUSABLE="NO"/>

<TRANSFORMATION NAME="SP_ENRICH_STATUS" TYPE="Stored Procedure">
    <TRANSFORMFIELD NAME="in_STATUS_CODE" DATATYPE="string" PRECISION="10" SCALE="0" PORTTYPE="INPUT"/>
    <TRANSFORMFIELD NAME="in_ORDER_COUNT" DATATYPE="number" PRECISION="10" SCALE="0" PORTTYPE="INPUT"/>
    <TRANSFORMFIELD NAME="out_ENRICHED_STATUS" DATATYPE="string" PRECISION="20" SCALE="0" PORTTYPE="OUTPUT"/>
    <TABLEATTRIBUTE NAME="Stored Procedure Name" VALUE="DW.ENRICH_CUSTOMER_STATUS"/>
</TRANSFORMATION>

<!-- Sequence Generator for ETL Timestamp -->
<INSTANCE NAME="SEQ_TIMESTAMP" TRANSFORMATION_NAME="SEQ_TIMESTAMP" TRANSFORMATION_TYPE="Sequence Generator" REUSABLE="NO"/>

<TRANSFORMATION NAME="SEQ_TIMESTAMP" TYPE="Sequence Generator">
    <TRANSFORMFIELD NAME="NEXTVAL" DATATYPE="number" PRECISION="10" SCALE="0" PORTTYPE="OUTPUT"/>
    <TRANSFORMFIELD NAME="ETL_TIMESTAMP" DATATYPE="date/time" PRECISION="19" SCALE="0" PORTTYPE="OUTPUT" EXPRESSION="SYSDATE"/>
</TRANSFORMATION>

<!-- Target Instance -->
<INSTANCE NAME="DIM_CUSTOMERS" TRANSFORMATION_NAME="DIM_CUSTOMERS" TRANSFORMATION_TYPE="Target Definition" REUSABLE="NO"/>

<!-- Connectors (data flow) -->
<!-- Source to SQ -->
<CONNECTOR FROMINSTANCE="CUSTOMERS" FROMINSTANCETYPE="Source Definition" FROMFIELD="CUSTOMER_ID" TOINSTANCE="SQ_CUSTOMER_ORDERS" TOINSTANCETYPE="Source Qualifier" TOFIELD="CUSTOMER_ID"/>
<CONNECTOR FROMINSTANCE="CUSTOMERS" FROMINSTANCETYPE="Source Definition" FROMFIELD="FIRST_NAME" TOINSTANCE="SQ_CUSTOMER_ORDERS" TOINSTANCETYPE="Source Qualifier" TOFIELD="FIRST_NAME"/>
<CONNECTOR FROMINSTANCE="CUSTOMERS" FROMINSTANCETYPE="Source Definition" FROMFIELD="LAST_NAME" TOINSTANCE="SQ_CUSTOMER_ORDERS" TOINSTANCETYPE="Source Qualifier" TOFIELD="LAST_NAME"/>
<CONNECTOR FROMINSTANCE="CUSTOMERS" FROMINSTANCETYPE="Source Definition" FROMFIELD="EMAIL" TOINSTANCE="SQ_CUSTOMER_ORDERS" TOINSTANCETYPE="Source Qualifier" TOFIELD="EMAIL"/>
<CONNECTOR FROMINSTANCE="CUSTOMERS" FROMINSTANCETYPE="Source Definition" FROMFIELD="STATUS_CODE" TOINSTANCE="SQ_CUSTOMER_ORDERS" TOINSTANCETYPE="Source Qualifier" TOFIELD="STATUS_CODE"/>

<!-- SQ to Expression -->
<CONNECTOR FROMINSTANCE="SQ_CUSTOMER_ORDERS" FROMINSTANCETYPE="Source Qualifier" FROMFIELD="CUSTOMER_ID" TOINSTANCE="EXP_TRANSFORM" TOINSTANCETYPE="Expression" TOFIELD="CUSTOMER_ID"/>
<CONNECTOR FROMINSTANCE="SQ_CUSTOMER_ORDERS" FROMINSTANCETYPE="Source Qualifier" FROMFIELD="FIRST_NAME" TOINSTANCE="EXP_TRANSFORM" TOINSTANCETYPE="Expression" TOFIELD="FIRST_NAME"/>
<CONNECTOR FROMINSTANCE="SQ_CUSTOMER_ORDERS" FROMINSTANCETYPE="Source Qualifier" FROMFIELD="LAST_NAME" TOINSTANCE="EXP_TRANSFORM" TOINSTANCETYPE="Expression" TOFIELD="LAST_NAME"/>
<CONNECTOR FROMINSTANCE="SQ_CUSTOMER_ORDERS" FROMINSTANCETYPE="Source Qualifier" FROMFIELD="EMAIL" TOINSTANCE="EXP_TRANSFORM" TOINSTANCETYPE="Expression" TOFIELD="EMAIL"/>
<CONNECTOR FROMINSTANCE="SQ_CUSTOMER_ORDERS" FROMINSTANCETYPE="Source Qualifier" FROMFIELD="STATUS_CODE" TOINSTANCE="EXP_TRANSFORM" TOINSTANCETYPE="Expression" TOFIELD="STATUS_CODE"/>
<CONNECTOR FROMINSTANCE="SQ_CUSTOMER_ORDERS" FROMINSTANCETYPE="Source Qualifier" FROMFIELD="ORDER_COUNT" TOINSTANCE="EXP_TRANSFORM" TOINSTANCETYPE="Expression" TOFIELD="ORDER_COUNT"/>
<CONNECTOR FROMINSTANCE="SQ_CUSTOMER_ORDERS" FROMINSTANCETYPE="Source Qualifier" FROMFIELD="TOTAL_REVENUE" TOINSTANCE="EXP_TRANSFORM" TOINSTANCETYPE="Expression" TOFIELD="TOTAL_REVENUE"/>

<!-- Expression to Stored Procedures -->
<CONNECTOR FROMINSTANCE="EXP_TRANSFORM" FROMINSTANCETYPE="Expression" FROMFIELD="o_CUSTOMER_ID" TOINSTANCE="SP_GET_CUSTOMER_KEY" TOINSTANCETYPE="Stored Procedure" TOFIELD="in_CUSTOMER_ID"/>
<CONNECTOR FROMINSTANCE="EXP_TRANSFORM" FROMINSTANCETYPE="Expression" FROMFIELD="CUSTOMER_STATUS" TOINSTANCE="SP_ENRICH_STATUS" TOINSTANCETYPE="Stored Procedure" TOFIELD="in_STATUS_CODE"/>
<CONNECTOR FROMINSTANCE="EXP_TRANSFORM" FROMINSTANCETYPE="Expression" FROMFIELD="o_ORDER_COUNT" TOINSTANCE="SP_ENRICH_STATUS" TOINSTANCETYPE="Stored Procedure" TOFIELD="in_ORDER_COUNT"/>

<!-- Expression/SP to Target -->
<CONNECTOR FROMINSTANCE="SP_GET_CUSTOMER_KEY" FROMINSTANCETYPE="Stored Procedure" FROMFIELD="out_CUSTOMER_KEY" TOINSTANCE="DIM_CUSTOMERS" TOINSTANCETYPE="Target Definition" TOFIELD="CUSTOMER_KEY"/>
<CONNECTOR FROMINSTANCE="EXP_TRANSFORM" FROMINSTANCETYPE="Expression" FROMFIELD="o_CUSTOMER_ID" TOINSTANCE="DIM_CUSTOMERS" TOINSTANCETYPE="Target Definition" TOFIELD="CUSTOMER_ID"/>
<CONNECTOR FROMINSTANCE="EXP_TRANSFORM" FROMINSTANCETYPE="Expression" FROMFIELD="FULL_NAME" TOINSTANCE="DIM_CUSTOMERS" TOINSTANCETYPE="Target Definition" TOFIELD="FULL_NAME"/>
<CONNECTOR FROMINSTANCE="EXP_TRANSFORM" FROMINSTANCETYPE="Expression" FROMFIELD="EMAIL_DOMAIN" TOINSTANCE="DIM_CUSTOMERS" TOINSTANCETYPE="Target Definition" TOFIELD="EMAIL_DOMAIN"/>
<CONNECTOR FROMINSTANCE="EXP_TRANSFORM" FROMINSTANCETYPE="Expression" FROMFIELD="o_ORDER_COUNT" TOINSTANCE="DIM_CUSTOMERS" TOINSTANCETYPE="Target Definition" TOFIELD="TOTAL_ORDERS"/>
<CONNECTOR FROMINSTANCE="EXP_TRANSFORM" FROMINSTANCETYPE="Expression" FROMFIELD="o_TOTAL_REVENUE" TOINSTANCE="DIM_CUSTOMERS" TOINSTANCETYPE="Target Definition" TOFIELD="LIFETIME_VALUE"/>
<CONNECTOR FROMINSTANCE="SP_ENRICH_STATUS" FROMINSTANCETYPE="Stored Procedure" FROMFIELD="out_ENRICHED_STATUS" TOINSTANCE="DIM_CUSTOMERS" TOINSTANCETYPE="Target Definition" TOFIELD="CUSTOMER_STATUS"/>
<CONNECTOR FROMINSTANCE="SEQ_TIMESTAMP" FROMINSTANCETYPE="Sequence Generator" FROMFIELD="ETL_TIMESTAMP" TOINSTANCE="DIM_CUSTOMERS" TOINSTANCETYPE="Target Definition" TOFIELD="ETL_TIMESTAMP"/>

</MAPPING>

<!-- Session -->
<SESSION NAME="s_customer_dimension" MAPPINGNAME="m_customer_dimension" ISVALID="YES" DESCRIPTION="Session for customer dimension load">
    <SESSTRANSFORMATIONINST TRANSFORMATIONNAME="CUSTOMERS" TRANSFORMATIONTYPE="Source Definition">
        <SESSIONCOMPONENT REFCONNECTIONNAME="CRM_ORACLE_CONN" COMPONENTNAME="CRM.CUSTOMERS"/>
    </SESSTRANSFORMATIONINST>
    <SESSTRANSFORMATIONINST TRANSFORMATIONNAME="DIM_CUSTOMERS" TRANSFORMATIONTYPE="Target Definition">
        <SESSIONCOMPONENT REFCONNECTIONNAME="DW_ORACLE_CONN" COMPONENTNAME="DW.DIM_CUSTOMERS"/>
    </SESSTRANSFORMATIONINST>
</SESSION>

<!-- Workflow -->
<WORKFLOW NAME="wf_customer_dimension_load" ISENABLED="YES" DESCRIPTION="Daily customer dimension ETL">
    <TASKINSTANCE NAME="Start" TASKTYPE="Start"/>
    <TASKINSTANCE NAME="s_customer_dimension" TASKTYPE="Session"/>
    <WORKFLOWLINK FROMTASK="Start" TOTASK="s_customer_dimension" CONDITION=""/>
</WORKFLOW>

</FOLDER>
</REPOSITORY>
</POWERMART>
"""


def example_basic_usage():
    """Basic usage example - parse XML and emit to DataHub."""
    print("=" * 70)
    print("Example 1: Basic Usage - Parse and Emit to DataHub")
    print("=" * 70)
    
    # Initialize extractor
    extractor = InformaticaLineageExtractor(
        datahub_server="http://localhost:8080",
        platform="oracle",
        platform_instance="prod",
        oracle_platform_instance="prod-oracle",
        env="PROD",
        emit_intermediate_jobs=True,
        use_dataset_for_procs=False,  # Reference procs as DataJobs
    )
    
    # Process XML
    folders = extractor.process_xml_string(SAMPLE_XML)
    
    # Print summary
    summary = extractor.get_lineage_summary()
    print(f"\nLineage Summary:")
    print(f"  Total edges: {summary['total_edges']}")
    print(f"  Source datasets: {summary['source_datasets']}")
    print(f"  Target datasets: {summary['target_datasets']}")
    print(f"  Stored procedures: {summary['stored_procedure_calls']}")
    
    # In production, emit to DataHub:
    # extractor.emit_to_datahub()
    
    return extractor


def example_detailed_lineage_inspection():
    """Detailed inspection of extracted lineage."""
    print("\n" + "=" * 70)
    print("Example 2: Detailed Lineage Inspection")
    print("=" * 70)
    
    extractor = InformaticaLineageExtractor(
        datahub_server="http://localhost:8080",
        platform="oracle",
        env="PROD",
    )
    
    extractor.process_xml_string(SAMPLE_XML)
    
    # Get all lineage edges
    edges = extractor.get_lineage_edges()
    
    print(f"\nDetailed Column Lineage Edges ({len(edges)} total):")
    print("-" * 70)
    
    # Group by target column
    edges_by_target = {}
    for edge in edges:
        target_key = f"{edge.target_dataset}.{edge.target_column}"
        if target_key not in edges_by_target:
            edges_by_target[target_key] = []
        edges_by_target[target_key].append(edge)
    
    for target_key, target_edges in sorted(edges_by_target.items()):
        print(f"\n→ {target_key}")
        
        # Sort by step order (reverse to show source first)
        sorted_edges = sorted(target_edges, key=lambda e: e.step_order, reverse=True)
        
        for edge in sorted_edges:
            trans_type = edge.transformation_type.value
            expr = edge.transformation_expression or "passthrough"
            if len(expr) > 50:
                expr = expr[:47] + "..."
            
            print(f"    ├─ [{trans_type}] {edge.source_dataset}.{edge.source_column}")
            print(f"    │     Expression: {expr}")
            print(f"    │     Confidence: {edge.confidence_score}")


def example_sql_parsing():
    """Example focusing on SQL parsing for Source Qualifier overrides."""
    print("\n" + "=" * 70)
    print("Example 3: SQL Parsing for Source Qualifier")
    print("=" * 70)
    
    from informatica_lineage import SQLLineageParser, FieldInfo
    
    parser = SQLLineageParser(dialect="oracle")
    
    # Sample SQL from Source Qualifier
    sql = """
    SELECT 
        c.CUSTOMER_ID,
        c.FIRST_NAME,
        c.LAST_NAME,
        c.EMAIL,
        c.STATUS_CODE,
        COUNT(o.ORDER_ID) AS ORDER_COUNT,
        SUM(o.TOTAL_AMOUNT) AS TOTAL_REVENUE
    FROM CRM.CUSTOMERS c
    LEFT JOIN CRM.ORDERS o ON c.CUSTOMER_ID = o.CUSTOMER_ID
    WHERE c.STATUS_CODE != 'DELETED'
    GROUP BY c.CUSTOMER_ID, c.FIRST_NAME, c.LAST_NAME, c.EMAIL, c.STATUS_CODE
    """
    
    # Define source table schemas
    source_tables = {
        "CRM.CUSTOMERS": {
            "CUSTOMER_ID": FieldInfo(name="CUSTOMER_ID", datatype="number"),
            "FIRST_NAME": FieldInfo(name="FIRST_NAME", datatype="string"),
            "LAST_NAME": FieldInfo(name="LAST_NAME", datatype="string"),
            "EMAIL": FieldInfo(name="EMAIL", datatype="string"),
            "STATUS_CODE": FieldInfo(name="STATUS_CODE", datatype="string"),
        },
        "CRM.ORDERS": {
            "ORDER_ID": FieldInfo(name="ORDER_ID", datatype="number"),
            "CUSTOMER_ID": FieldInfo(name="CUSTOMER_ID", datatype="number"),
            "TOTAL_AMOUNT": FieldInfo(name="TOTAL_AMOUNT", datatype="decimal"),
        },
    }
    
    # Parse SQL
    lineage_results = parser.parse_sql(sql, source_tables)
    
    print("\nParsed Column Lineage from SQL:")
    print("-" * 50)
    
    for upstream_cols, output_col, expr in lineage_results:
        print(f"\n  Output: {output_col}")
        print(f"  Expression: {expr}")
        print(f"  Upstream columns:")
        for table, col in upstream_cols:
            print(f"    - {table}.{col}")
    
    # Detect stored procedure calls
    proc_sql = "BEGIN PKG_CUSTOMER.GET_OR_CREATE_KEY(:1, :2); END;"
    proc_result = parser.detect_stored_procedure_call(proc_sql)
    
    print(f"\n\nStored Procedure Detection:")
    print(f"  SQL: {proc_sql}")
    print(f"  Detected: {proc_result}")


def example_stored_procedure_references():
    """Example showing stored procedure cross-references."""
    print("\n" + "=" * 70)
    print("Example 4: Stored Procedure Cross-References")
    print("=" * 70)
    
    extractor = InformaticaLineageExtractor(
        datahub_server="http://localhost:8080",
        platform="oracle",
        platform_instance="prod",
        oracle_platform_instance="prod-oracle",
        env="PROD",
        use_dataset_for_procs=False,  # Use DataJob URNs
    )
    
    extractor.process_xml_string(SAMPLE_XML)
    
    edges = extractor.get_lineage_edges()
    
    # Find stored procedure edges
    proc_edges = [e for e in edges if e.transformation_type == TransformationType.STORED_PROCEDURE]
    
    print(f"\nStored Procedure References ({len(proc_edges)} calls):")
    print("-" * 50)
    
    for edge in proc_edges:
        print(f"\n  Procedure: {edge.source_dataset}")
        print(f"  Expression: {edge.transformation_expression}")
        print(f"  Target: {edge.target_dataset}.{edge.target_column}")
    
    print("\n\nDataHub URN formats for Oracle procedures:")
    print("-" * 50)
    
    # Show both URN formats
    from datahub.emitter.mce_builder import make_data_flow_urn, make_data_job_urn_with_flow, make_dataset_urn
    
    seen_procs = set()
    for edge in proc_edges:
        proc_name = edge.source_dataset
        if proc_name in seen_procs:
            continue
        seen_procs.add(proc_name)
        
        # DataJob URN format
        flow_urn = make_data_flow_urn("oracle", "stored_procedures", "PROD")
        job_urn = make_data_job_urn_with_flow(flow_urn, proc_name)
        print(f"\n  Procedure: {proc_name}")
        print(f"  As DataJob: {job_urn}")
        
        # Dataset URN format (Oracle connector default)
        dataset_urn = make_dataset_urn("oracle", proc_name, "PROD")
        print(f"  As Dataset: {dataset_urn}")


def example_transformation_chain():
    """Example showing ordered transformation chains."""
    print("\n" + "=" * 70)
    print("Example 5: Ordered Transformation Chains")
    print("=" * 70)
    
    parser = InformaticaXMLParser()
    folders = parser.parse_string(SAMPLE_XML)
    
    folder = folders["CUSTOMER_ETL"]
    mapping = folder.mappings["m_customer_dimension"]
    
    builder = LineageBuilder(
        folder=folder,
        platform="oracle",
        env="PROD",
    )
    
    # Build transformation chains
    chains = builder.build_mapping_lineage(mapping)
    
    print("\nTransformation Chains by Target Field:")
    print("-" * 50)
    
    for target_field, steps in sorted(chains.items()):
        print(f"\n→ {target_field}")
        
        # Sort steps by order (source first)
        sorted_steps = sorted(steps, key=lambda s: s.step_order, reverse=True)
        
        for i, step in enumerate(sorted_steps):
            prefix = "  └─" if i == len(sorted_steps) - 1 else "  ├─"
            print(f"{prefix} Step {step.step_order}: {step.instance_name} ({step.transformation_type.value})")
            
            if step.expression:
                expr = step.expression[:40] + "..." if len(step.expression) > 40 else step.expression
                print(f"       Expression: {expr}")
            
            if step.input_fields:
                print(f"       Inputs: {step.input_fields}")


def example_datahub_emission_preview():
    """Preview DataHub MCPs without actually emitting."""
    print("\n" + "=" * 70)
    print("Example 6: DataHub MCP Preview")
    print("=" * 70)
    
    extractor = InformaticaLineageExtractor(
        datahub_server="http://localhost:8080",
        platform="oracle",
        platform_instance="prod",
        env="PROD",
        emit_intermediate_jobs=True,
    )
    
    extractor.process_xml_string(SAMPLE_XML)
    
    # Get the accumulated MCPs
    mcps = extractor.emitter.mcps
    
    print(f"\nGenerated {len(mcps)} Metadata Change Proposals:")
    print("-" * 50)
    
    # Group by entity type
    mcp_by_type = {}
    for mcp in mcps:
        entity_type = mcp.entityUrn.split(":")[2]
        if entity_type not in mcp_by_type:
            mcp_by_type[entity_type] = []
        mcp_by_type[entity_type].append(mcp)
    
    for entity_type, type_mcps in mcp_by_type.items():
        print(f"\n{entity_type} ({len(type_mcps)} MCPs):")
        for mcp in type_mcps[:3]:  # Show first 3
            print(f"  - {mcp.entityUrn}")
            print(f"    Aspect: {type(mcp.aspect).__name__}")
        if len(type_mcps) > 3:
            print(f"  ... and {len(type_mcps) - 3} more")


def main():
    """Run all examples."""
    print("\n" + "=" * 70)
    print("INFORMATICA POWERCENTER TO DATAHUB LINEAGE EXAMPLES")
    print("=" * 70)
    
    # Run examples
    example_basic_usage()
    example_detailed_lineage_inspection()
    
    # SQL parsing requires sqlglot
    try:
        import sqlglot
        example_sql_parsing()
    except ImportError:
        print("\n[Skipping SQL parsing example - sqlglot not installed]")
    
    example_stored_procedure_references()
    example_transformation_chain()
    example_datahub_emission_preview()
    
    print("\n" + "=" * 70)
    print("All examples completed!")
    print("=" * 70)


if __name__ == "__main__":
    main()
