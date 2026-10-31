# KES SPARK Connector

- Branch: **kingbasev3**
- Upstream: https://github.com/apache/doris-spark-connector
- Upstream tag: **26.1.0**, commit: a25f7a1845760e45111e7eccac2f4087601cd563
- KES Maven group: **com.kingbase.kes**
- KES artifact prefix: **spark-kes-connector**
- KES version: **26.1.0-kes.1**
- Target database: KingbaseES based on Apache Doris 4.1.5
- New customer-facing identifier: **kes**, and no other KES identifier
- Legacy identifier: **doris** remains registered for migration compatibility

## Compatibility rules

Upstream Java packages (org.apache.doris), thrift-service (org.apache.doris),
Flink/Spark public APIs, shade/relocation paths, HTTP, Thrift, Arrow Flight,
Stream Load headers, and regression fixture JAR main classes are external
contracts. Do not rename these mechanically.

The original Apache License/NOTICE/source copyrights remain intact.
Only self-owned KES Connector Maven coordinates and the new alias are changed.
Original upstream module directory names remain stable for easy upstream diff.

## Invocation

Spark DataFrame: spark.read.format("kes")
 .option("kes.fenodes", "host:8030")
 .option("kes.table.identifier", "db.table")
 .option("kes.user", "user").option("kes.password", "...").load().
 The existing .format("doris") and doris.* options remain valid; kes.*
 options normalize into canonical internal Doris parser keys.

## Release gates

Compile and test the selected Flink/Spark and JDK-specific module from its
existing directory. Validate KES and legacy Doris SPI, read/write, types,
retries, checkpoints, TLS, errors and CDC DELETE (Flink), then test the
packaged JAR against an isolated KingbaseES 4.1.5-derived Coordinator/DataNode
deployment. Check actual HTTP/SQL/Thrift endpoints and auth, not planned
URLs. Do not overwrite original regression JARs, change golden, deploy to
production or push branches without explicit approval.

Passing XML/static checks is not sufficient for a production release.
