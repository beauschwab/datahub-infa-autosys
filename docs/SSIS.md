# SSIS metadata extraction to OpenLineage and DataHub: A complete implementation guide

**No official connectors exist for SSIS lineage in either OpenLineage or DataHub**, but a robust solution can be built by combining open-source SSIS parsing tools, SQLGlot for column-level lineage analysis, and DataHub's Python SDK for emission. This guide documents working implementations, code patterns, and practical approaches for building a complete SSIS-to-DataHub lineage pipeline.

## Open source SSIS .dtsx parsing implementations

Several mature Python projects parse SSIS package XML and extract transformation metadata. **SSISMig** (github.com/melissachen09/SSISMig) provides the most comprehensive solution, converting SSIS packages to Airflow DAGs and dbt projects through a three-stage architecture: `.dtsx XML → Parser → Intermediate Representation (JSON) → Generators`. It handles encrypted packages, supports Execute SQL Tasks, Data Flow Tasks, OLE DB Sources, Derived Columns, and Lookups, mapping each to modern equivalents.

For lighter-weight SQL extraction, **bakerjd99's lxml approach** demonstrates the core parsing pattern:

```python
from lxml import etree

tree = etree.parse("package.dtsx")
root = tree.getroot()

# Key SSIS XML namespaces
pfx = '{www.microsoft.com/'
exe_tag = pfx + 'SqlServer/Dts}Executable'
dat_tag = pfx + 'SqlServer/Dts}ObjectData'
tsk_tag = pfx + 'sqlserver/dts/tasks/sqltask}SqlTaskData'
src_tag = pfx + 'sqlserver/dts/tasks/sqltask}SqlStatementSource'

for ele in root.xpath(".//*"):
    if ele.tag == exe_tag:
        for child0 in ele:
            if child0.tag == dat_tag:
                for child1 in child0:
                    if child1.tag == tsk_tag:
                        sql = child1.attrib.get(src_tag)  # Embedded SQL
```

The **Extract-SSIS-SQL-Scripts** repository (github.com/adamcraven2/Extract-SSIS-SQL-Scripts) documents precise XML paths for both Execute SQL Tasks and Data Flow Task SqlCommand properties. For enterprise dependency analysis, **SQLServerMetadata** (C#, 86 stars) reads packages from file systems, SSIS Service, or SSIS Catalog, extracting data flow metadata with support for SQL Server 2005-2017.

### Critical XML structure reference

**Execute SQL Task** embeds SQL in `SQLTask:SqlStatementSource`:
```xml
<DTS:Executable DTS:ExecutableType="Microsoft.ExecuteSQLTask">
  <DTS:ObjectData>
    <SQLTask:SqlTaskData SQLTask:SqlStatementSource="SELECT * FROM Table"/>
  </DTS:ObjectData>
</DTS:Executable>
```

**OLE DB Source** uses the `SqlCommand` property within pipeline components:
```xml
<pipeline><components><component>
  <properties>
    <property name="SqlCommand">SELECT id, name FROM customers</property>
  </properties>
</component></components></pipeline>
```

**Connection managers** contain server/database details in `DTS:ConnectionString` attributes. The official MS-DTSX specification (learn.microsoft.com/en-us/openspecs/sql_data_portability/ms-dtsx/) provides the complete XML schema.

## SQLGlot delivers column-level lineage from embedded SQL

**SQLGlot** is the primary recommendation for CLL extraction from SSIS SQL, offering a native T-SQL dialect, dedicated `lineage` module, and no external dependencies. It parses SQL into an AST and traces column derivations through JOINs, CTEs, subqueries, CASE expressions, and window functions.

```python
from sqlglot.lineage import lineage
from sqlglot import MappingSchema

# Provide schema for accurate column resolution
schema = MappingSchema({
    "customers": {"id": "int", "name": "varchar", "email": "varchar"},
    "orders": {"id": "int", "customer_id": "int", "amount": "decimal"}
})

sql = """
SELECT u.name, SUM(o.amount) as total_spend
FROM customers u
JOIN orders o ON u.id = o.customer_id
GROUP BY u.name
"""

# Trace lineage for specific output column
result = lineage("total_spend", sql, schema=schema, dialect="tsql")
for node in result.walk():
    print(f"{node.name} <- {node.source}")  
# Output: total_spend <- orders.amount (via SUM aggregation)
```

For complex transformations, use `qualify` to resolve ambiguous column references:
```python
from sqlglot.optimizer.qualify import qualify
from sqlglot import parse_one

ast = parse_one(sql, dialect="tsql")
qualified = qualify(ast, schema=schema, dialect="tsql")
# All columns now prefixed with source tables
```

**Python-sqllineage** offers an alternative with CLI support: `sqllineage -f query.sql --dialect=tsql -l column`. **DataHub's own SQL parser** (built on a SQLGlot fork) achieves **97-99% accuracy** with schema awareness and is accessible via `datahub.sql_parsing.sqlglot_lineage`.

### Handling T-SQL specific constructs

SQLGlot handles T-SQL syntax including `TOP`, `CONVERT`, `ISNULL`, and `WITH (NOLOCK)` hints. Stored procedures require pre-processing to extract individual statements—dynamic SQL (`EXEC(@sql)`) cannot be parsed statically. Parse each DML statement separately and flag dynamic portions for manual review.

## OpenLineage column-level lineage schema and Python SDK

The **ColumnLineageDatasetFacet** structure represents column derivations as output dataset facets:

```json
{
  "columnLineage": {
    "_schemaURL": "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json",
    "fields": {
      "target_column": {
        "inputFields": [{
          "namespace": "sqlserver://server:1433/db",
          "name": "schema.source_table",
          "field": "source_column",
          "transformations": [{
            "type": "DIRECT",
            "subtype": "TRANSFORMATION",
            "masking": false
          }]
        }]
      }
    }
  }
}
```

Transformation types include **DIRECT** (IDENTITY, TRANSFORMATION, AGGREGATION) and **INDIRECT** (JOIN, FILTER, GROUP_BY, SORT, WINDOW, CONDITIONAL).

Creating OpenLineage events programmatically with `openlineage-python`:

```python
from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import InputDataset, OutputDataset, Job, Run, RunEvent, RunState
from openlineage.client.facet_v2 import column_lineage_dataset, schema_dataset

client = OpenLineageClient(url="http://localhost:5000")

# Build column lineage facet
col_lineage = column_lineage_dataset.ColumnLineageDatasetFacet(
    fields={
        "full_name": column_lineage_dataset.Fields(
            inputFields=[
                column_lineage_dataset.InputField(
                    namespace="sqlserver://source:1433/db",
                    name="dbo.customers",
                    field="first_name"
                ),
                column_lineage_dataset.InputField(
                    namespace="sqlserver://source:1433/db", 
                    name="dbo.customers",
                    field="last_name"
                )
            ],
            transformationDescription="CONCAT(first_name, ' ', last_name)"
        )
    }
)

output = OutputDataset(
    namespace="sqlserver://target:1433/dw",
    name="staging.dim_customer",
    facets={"columnLineage": col_lineage}
)
```

## DataHub integration requires custom source implementation

**DataHub has no official SSIS connector.** Organizations must build custom integrations using the Python SDK, mapping SSIS concepts to DataHub entities:

| SSIS Concept | DataHub Entity |
|-------------|----------------|
| SSIS Package | DataFlow |
| Data Flow Task | DataJob |
| Execute SQL Task | DataJob |
| Source/Destination | Dataset (via inputDatasets/outputDatasets) |

**Creating DataFlow and DataJob with lineage:**

```python
from datahub.sdk import DataFlow, DataJob, DataHubClient
from datahub.metadata.urns import DatasetUrn

client = DataHubClient.from_env()

# Package becomes DataFlow
dataflow = DataFlow(
    platform="ssis",
    name="CustomerETL_Package",
    platform_instance="PROD_SERVER"
)

# Data Flow Task becomes DataJob with lineage
datajob = DataJob(
    name="Load_DimCustomer",
    flow=dataflow,
    inlets=[DatasetUrn(platform="mssql", name="staging.raw_customers")],
    outlets=[DatasetUrn(platform="mssql", name="dw.dim_customer")]
)

client.entities.upsert(dataflow)
client.entities.upsert(datajob)
```

For column-level lineage, use the lower-level MCP API with `DataJobInputOutputClass` or emit via DataHub's OpenLineage endpoint at `POST /openapi/openlineage/api/v1/lineage`.

### Building a custom SSIS ingestion source

```python
from datahub.ingestion.api.source import Source
from datahub.ingestion.api.decorators import platform_name, config_class

@platform_name("SSIS")
@config_class(SSISSourceConfig)
class SSISSource(Source):
    def get_workunits_internal(self):
        for package in self._parse_ssis_packages():
            # Emit DataFlow for package
            yield self._create_dataflow_workunit(package)
            
            # Emit DataJob for each Data Flow Task
            for task in package.data_flow_tasks:
                yield self._create_datajob_workunit(task)
```

## Similar ETL tool integrations provide architectural patterns

**Apache NiFi** (official DataHub connector) maps Process Groups to DataFlows and Processors to DataJobs, extracting lineage from provenance events. **Apache Airflow** uses an event listener plugin capturing DAG executions automatically. **Apache Spark** leverages OpenLineage integration with automatic column-level lineage from logical plans.

Neither **Talend** nor **Informatica** have official DataHub connectors—they require custom implementations similar to what's needed for SSIS. Commercial tools like **MANTA**, **Collibra**, and **Microsoft Purview** provide automated SSIS lineage scanning as alternatives.

## Complete implementation approach

A practical SSIS-to-DataHub pipeline combines these components:

1. **Parse DTSX XML** using lxml or SSISMig patterns to extract SQL statements, connection info, and component metadata
2. **Analyze SQL with SQLGlot** (`dialect="tsql"`) to extract column-level lineage from embedded queries
3. **Map to OpenLineage/DataHub entities**: Package → DataFlow, Tasks → DataJob, column mappings → ColumnLineageDatasetFacet
4. **Emit via DataHub SDK** using `DataFlow`, `DataJob`, and lineage APIs

```python
def process_ssis_package(dtsx_path, datahub_client):
    # 1. Parse package XML
    tree = etree.parse(dtsx_path)
    package_name = extract_package_name(tree)
    
    # 2. Create DataFlow
    dataflow = DataFlow(platform="ssis", name=package_name)
    datahub_client.entities.upsert(dataflow)
    
    # 3. Extract and process Data Flow Tasks
    for task in extract_data_flow_tasks(tree):
        sql = task.get("SqlCommand")
        
        # 4. Analyze SQL for column lineage
        if sql:
            col_lineage = analyze_column_lineage(sql, schema, dialect="tsql")
        
        # 5. Create DataJob with lineage
        datajob = DataJob(
            name=task["name"],
            flow=dataflow,
            inlets=[...],  # From source components
            outlets=[...]  # From destination components
        )
        datahub_client.entities.upsert(datajob)
```

### Key repositories and resources

- **SSISMig** (Python SSIS parser): github.com/melissachen09/SSISMig
- **Extract-SSIS-SQL-Scripts**: github.com/adamcraven2/Extract-SSIS-SQL-Scripts
- **SQLServerMetadata** (C# analyzer): github.com/keif888/SQLServerMetadata
- **SQLGlot lineage docs**: sqlglot.com/sqlglot/lineage.html
- **OpenLineage spec**: openlineage.io/docs/spec/facets/dataset-facets/column_lineage_facet
- **DataHub custom sources**: docs.datahub.com/docs/metadata-ingestion/adding-source
- **MS-DTSX specification**: learn.microsoft.com/en-us/openspecs/sql_data_portability/ms-dtsx/

## Conclusion

Building SSIS-to-DataHub lineage requires assembling existing open-source components rather than using turnkey solutions. The combination of **lxml-based DTSX parsing**, **SQLGlot's T-SQL column lineage analysis**, and **DataHub's DataFlow/DataJob APIs** provides a complete toolkit. SSISMig demonstrates production-ready SSIS parsing patterns, while DataHub's NiFi and Airflow connectors offer architectural blueprints for custom ETL integrations. The critical insight is that column-level lineage accuracy depends heavily on providing schema context to SQLGlot—without table definitions, column resolution in JOINs and subqueries becomes ambiguous.