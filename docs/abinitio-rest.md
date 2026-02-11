# Ab Initio REST API capabilities for metadata extraction

**Ab Initio's closed ecosystem offers limited public REST API documentation, with native REST support only available in version 4.0 and later.** Organizations seeking programmatic metadata access must rely primarily on command-line interfaces (air commands), EME web services, or the Kubernetes Operator—there are no public SDKs or open-source API wrappers available. For enterprise metadata extraction, the most viable paths are direct database queries to the EME repository, file-based exports, or custom integration development.

## REST API support arrived with Ab Initio 4.0

Ab Initio 4.0 (released circa 2020) marked a significant architectural shift by introducing native **RESTful API and microservices support** with OpenAPI standard compliance. Prior versions relied exclusively on SOAP web services with WSDL specifications.

The 4.0 release enabled:
- Microservices that scale horizontally with containerization support
- OpenAPI-compliant service definitions
- Service mesh technology integration
- Data virtualization accessible via **ODBC, JDBC, and Web Services** through Data>Catalog

However, a critical limitation persists: **specific REST endpoints are not publicly documented**. Ab Initio maintains a proprietary documentation model where technical specifications, Swagger files, and API references are only accessible through the authenticated "Forum" portal for licensed customers.

| Version | API Capabilities |
|---------|-----------------|
| 2.x/3.x | SOAP/WSDL web services, air commands only |
| 4.0+ | REST APIs, OpenAPI support, microservices, Kubernetes operator |
| 4.3.x | Current latest with Authorization Gateway APIs |

## Control>Center provides operational metadata access

Control>Center serves as Ab Initio's job monitoring and operational management hub. While it doesn't expose publicly documented REST endpoints, it provides several programmatic access pathways.

**OPDB Database queries** offer the most direct route to execution metadata. The Operational Database stores job statistics including start time, end time, duration, status, file receive timestamps, and execution statistics. Organizations can query this database directly using SQL once they understand the schema structure.

**Authentication** integrates with enterprise LDAP/Active Directory systems and supports role-based access control (restricted and admin roles). Specific OAuth or API key documentation is not publicly available.

For job control operations, the platform supports: starting, stopping, scheduling, rerunning, disabling, enabling, holding, and releasing jobs—all accessible through the Control>Center interface. SLA tracking, CPU/memory metrics, and alerting capabilities round out the operational feature set.

## EME exposes metadata through multiple interfaces

The Enterprise Meta>Environment functions as Ab Initio's central metadata repository, storing graphs, DML files, transformations, shell scripts, parameters, and their relationships. EME operates as an **object-oriented data storage system** rather than a traditional relational database.

**Air commands** represent the primary programmatic interface:

```bash
# List objects in EME directory
air object ls <EME_path_to_directory>

# View object version history with full details
air object versions-verbose <EME_path_to_object>

# Display object contents
air object cat <EME_path_to_object>

# Import project files to EME
air project import /Projects/COMPANY/APP/SANDBOX -basedir /ai/src/APP/SANDBOX -files mp/graph.mp
```

**EME web services APIs** enable external systems to query business metadata, submit metadata change requests, subscribe to metadata changes, and export in CWM XMI format. The web interface is accessible at `http://serverhost:[serverport]/abinitio` for browser-based metadata browsing and editing.

**Database backend options** include Oracle, Teradata, SQL Server, DB2, and PostgreSQL. Organizations extending the EME schema to these databases can execute direct SQL queries for metadata extraction—a common workaround when REST APIs are unavailable.

The automatic **dependency analysis** capability traces data flow component-to-component and field-by-field across graphs, parsing SQL statements to link table columns with output record formats.

## Metadata Hub offers lineage and governance APIs

Metadata Hub provides the richest metadata access capabilities in the Ab Initio ecosystem, supporting both business and technical lineage views.

**The mh-import command-line interface** handles metadata operations:

```bash
# Import rules and feeds
mh-import rule save <rule_name>.rule
mh-import feed save <feed_name>

# Configure site parameters
mh-import site-parameters add -name MHUB_ENV_DATA_ROOT -value /path/on/disk

# Load Data Quality metrics with authentication
mh-import dq-load \
  -issue-counts-file $OUTPUT_DIR/dq-issue-count.dat \
  -metric-scores-file $OUTPUT_DIR/dq-metric-score.dat \
  -ds-info-file $OUTPUT_DIR/dq-dataset-info.dat \
  -a $MHUB_URL -u $USERNAME -p $PASSWORD
```

**Extractors** import metadata from third-party systems including ERwin data models, SQL/BTEQ scripts, and other BI tools. Custom extractors can be developed using Ab Initio's documentation, executed via UI or command line, and scheduled for automated runs.

Technical lineage is **auto-generated** from metadata imports, while business lineage requires manual curation. The integration with DQE (Data Quality Environment) overlays quality metrics onto lineage views.

## Kubernetes Operator enables modern programmatic access

The **Co>Operating System Runtime Operator**—certified on Red Hat OpenShift—represents Ab Initio's most modern programmatic interface. It defines custom Kubernetes API resources for provisioning and running data-processing jobs, enabling:

- Container-orchestrated graph execution
- Native Kubernetes API job submission
- DevOps and CI/CD workflow integration
- OCI-compliant container image creation via Buildpacks

This operator bypasses the need for traditional REST APIs by leveraging Kubernetes-native patterns for job control.

**Jenkins integration** ships natively with Ab Initio 4.0, supporting automated test execution, continuous regression testing, and CI/CD pipeline implementation with unit test generation from baseline runs.

## No open-source wrappers or third-party connectors exist

Research across GitHub, PyPI, and npm reveals **no community-built REST wrappers, metadata extractors, or API connectors** for Ab Initio. The term "ab initio" in open-source contexts refers to computational chemistry ("from first principles"), not the ETL platform.

**Major data governance tools lack native Ab Initio support:**

| Platform | Native Connector | Integration Path |
|----------|-----------------|------------------|
| Informatica EDC | ❌ Not available | Custom Resource Scanner development |
| Collibra | ❌ Not available | REST Import API custom development |
| Alation | ❌ Not available | Custom connector required |
| Apache Atlas | ❌ Not available | REST API/Kafka custom hook |

Organizations must either use Ab Initio's native governance tools (EME, Metadata Hub, Data>Catalog) or invest in custom integration development.

**Common workaround patterns include:**
- Exporting EME metadata to a relational database, then scanning with governance tools
- Parsing Ab Initio graph XML/proprietary formats programmatically
- Building middleware that translates EME web services to target catalog APIs
- CSV/Excel export from EME for manual import to governance platforms

## Practical implementation approaches

For organizations requiring Ab Initio metadata extraction without native REST APIs, several proven patterns exist:

**CLI-to-REST bridge architecture**: Build a wrapper service using Flask (Python) or Spring Boot (Java) that accepts REST requests, invokes air commands via subprocess calls, and returns structured JSON responses. This enables REST-style access without waiting for official API availability.

**Message queue integration**: Ab Initio natively connects to Apache Kafka, Amazon Kinesis, Google Pub/Sub, Azure Message Bus, and JMS/MQ. Event-driven architectures can trigger metadata extraction workflows without custom APIs.

**Database-level access**: When EME stores its schema in Oracle or Teradata, direct SQL queries extract metadata. A Fortune 500 financial services company successfully implemented end-to-end data lineage by querying EME database tables directly and building custom reports for source-to-target lineage extraction.

## Conclusion

Ab Initio's API ecosystem prioritizes enterprise security over public accessibility. REST API capabilities exist in version 4.0+ but remain behind proprietary documentation. **The most practical metadata extraction paths are air commands for automation, direct database queries to EME repositories, and the Kubernetes Operator for containerized environments.** Organizations should expect custom development work for any integration with third-party data catalogs—no turnkey solutions exist. For definitive API specifications, direct engagement with Ab Initio support (support@abinitio.com) or licensed Forum access is required.