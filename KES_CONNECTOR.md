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

## Coordinator and DataNode discovery

FE discovery uses the KES management endpoint
`GET /rest/v2/manager/node/coordinators` (HTTP or HTTPS according to the
connector TLS configuration). The response is wrapped in `data` and contains
`columnNames` plus `rows`; the connector resolves `Host`, `HttpPort`,
`QueryPort` and `ArrowFlightSqlPort` by column name. The endpoint is
authenticated and applies information-schema read authorization. Do not use
the upstream `/rest/v2/manager/node/frontends` URL.

DataNode discovery is a separate authenticated request:
`GET /api/datanodes?is_alive=true`, returning `data.datanodes` with `ip`,
`http_port` and `is_alive`. The connector must use this contract for backend
selection; it is not a replacement for Coordinator discovery.

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

## Thrift SDK decision (2026-10-31: migration deferred)

**Continue using the Apache Doris SDK for now.** The Base module depends on
`org.apache.doris:thrift-service:1.0.1`, supplying the generated
`org.apache.doris.sdk.thrift` RPC classes (235 in inspected fat JARs).
This external protocol binding is **not** a legacy Doris DataSource or
`doris.*` options compatibility layer. Do not rename/exclude it,
invent a KES Maven dependency or modify the Thrift wire contract.

KES SDK migration is **deferred / NOT_RUN** until a buildable SDK exists,
wire compatibility is verified against Coordinator/DataNode, and Spark
3.5/4.1 builds and real read/write, RPC error and auth paths pass.
LICENSE/NOTICE attribution is retained. The independent `spark-load/` module is migrated to KES names but remains separate from the main Connector; regression fixtures are not released in
the KES Spark Connector artifact.

Before release, verify real read/write, metadata/schema, streaming/checkpoint,
retries, TLS/auth negative paths and restart behavior on isolated CN/DN.
No production deployment or Git push is authorized by this document.

## Residual identity cleanup (2026-11-03)

The Connector's 11 dependency relocation destinations use `com.kingbase.kes.shaded`.
The integration-test artifact property is `spark.kes.connector.artifactId`.
The shared `env.sh` consumes `KES_HOME`, matching both build entry points.
Source archives use the local committed HEAD and a KES filename; they do not publish a release.

The independent `spark-load/` extension now uses `com.kingbase.kes`, `KesClient`,
`KesKryoRegistrator` and `KesRangePartitioner` consistently in Java paths, Maven
coordinates, the launcher, reflection strings, tests and assembly dependencies.
DPP shading has one relocation per package and uses Guava's actual `com.google.common` package.
New extension jobs generate `kes_global_dict_table_*`, `kes_distinct_key_table_*`
and `kes_intermediate_hive_table_*` Hive names. Old jobs, serialized Java state and
existing Hive dictionaries are not migrated or deleted; finish such jobs with their original version.
JSON field names and HTTP wire paths are unchanged.

**UNSUPPORTED_WITH_CURRENT_KERNEL:** this separate Spark Load extension requires
`/api/ingestion_load/{catalog}/{db}/_create`, `/_update`, and `/api/{db}/_load_info`.
These HTTP handlers do not exist in the current Doris 4.1.5-derived KES kernel.
Its source still participates in its existing CI build, but it is not a compatible
runtime deliverable for that kernel. Porting its load protocol requires a separate
functional change. The main Spark Connector uses its own Stream Load/read paths.
This cleanup does not establish runtime support for the separate extension.

Validation is source-only: package/import/resource links, XML/JSON, shell syntax,
changed Java line widths and references. No build, JUnit, E2E or cluster test ran.
Real Thrift SDK coordinates/imports and upstream attribution are preserved.

## Fork CI branch coverage (2026-11-04)

The license workflow accepts pushes to `kingbasev3` as well as `master`; existing PR coverage remains. Other build/test workflows already accept the fork branch. The independent Spark Load protocol port is explicitly deferred by the user for this round. This change does not disable its existing build CI or change its source. No CI run or compilation was performed locally.
