# KES Spark Connector

KES-only Spark integration for KingbaseES, derived from Apache Doris Spark
Connector 26.1.0.

Use spark-kes-connector as the project root, com.kingbase.kes Maven
coordinates and Spark DataSource format("kes"). Only explicitly prefixed
kes.* configuration keys are supported.

For build profiles, a read/write example, third-party dependency boundaries,
and incomplete release gates, see [KES Spark Connector Guide](KES_CONNECTOR.md).

The historical upstream regression fixtures and separate Spark Load extension
are outside the two primary KES Connector shipping jars. Third-party source
attribution and license notices are preserved in LICENSE and NOTICE.


## KES Coordinator discovery contract (2026-11-01)

Spark FE auto-discovery and Arrow Flight SQL port refresh use the KES endpoint
`GET /rest/v2/manager/node/coordinators`. It returns `data.columnNames` and
`data.rows`, including `Host`, `HttpPort`, `QueryPort` and
`ArrowFlightSqlPort`. The previous `/rest/v2/manager/node/frontends` path is
not a KES endpoint and is not retained as an alias. The endpoint requires
authentication and the caller's information-schema read authorization.

The DataNode discovery endpoint remains separately
`GET /api/datanodes?is_alive=true`, returning `data.datanodes` with
`ip`/`http_port`/`is_alive`. These are distinct contracts and must not be
conflated. Verify both URLs and response shapes when changing the Coordinator
or either connector.

## KES DataNode discovery contract (2026-10-31)

Use the paired new Coordinator: GET /api/datanodes?is_alive=true returns data.datanodes with ip/http_port/is_alive. The dedicated discovery API requires authentication, not the ADMIN-only management API. No legacy URL or JSON alias is provided. Rebuild and roll out Coordinator and Connector JARs together; verify artifact SHA256 rather than version names alone.

The parent forces fresh input JAR creation before shading, avoiding old classes from an earlier shaded artifact during incremental packaging. Tests exercise the new HTTPS URL and DTO, including old-field rejection. Real cluster/Stream Load/CDC and compute-group management acceptance remain separate.
