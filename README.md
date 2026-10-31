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
