# KES Spark Connector (KES-only)

Source adapted from Apache Doris Spark Connector 26.1.0 to connect Spark
to KingbaseES (Apache Doris 4.1.5-derived product).

## Runtime contract

- Maven groupId: com.kingbase.kes
- Artifact prefix: spark-kes-connector
- Source module root: spark-kes-connector
- Owned Java and Scala namespace: com.kingbase.kes.spark
- Spark DataSource identifier: kes only
- Config options: explicitly kes.* only; plain or Doris-prefixed keys are rejected
- Supported build candidates: Spark 3.5 / Scala 2.12 / JDK 17 and Spark 4.1
  with the profile-specific Scala/JDK requirements

There is no legacy DataSource alias or option normalization to Doris keys.

## Spark DataFrame read/write

~~~python
df = (spark.read.format("kes")
    .option("kes.fenodes", "coordinator-host:8030")
    .option("kes.table.identifier", "qa_db.kes_sink")
    .option("kes.user", "root")
    .option("kes.password", "")
    .load())

(df.write.format("kes")
    .option("kes.fenodes", "coordinator-host:8030")
    .option("kes.table.identifier", "qa_db.kes_sink")
    .option("kes.user", "root")
    .option("kes.password", "")
    .mode("append").save())
~~~

## Building

From the original spark-kes-connector directory, use the build.sh profile
for the target Spark version, or build with Maven
-Pspark-3.5 -pl spark-kes-connector-spark-3.5 -am
and similarly -Pspark-4.1 for Spark 4.1. The Spark Scala plugin may require
the standard install lifecycle for KES reactor modules before packaging.

The external org.apache.doris:thrift-service and
org.apache.doris.sdk.thrift bindings remain third-party protocol dependencies,
NOT a legacy plugin/API alias. LICENSE, NOTICE and original ASF attribution
are retained. The independent upstream spark-load source tree and regression
fixtures are not included in the KES Spark Connector artifact.

Before release, verify real read/write, metadata/schema, streaming/checkpoint,
retries, TLS/auth negative paths and restart behavior on isolated CN/DN.
No production deployment or Git push is authorized by this document.
