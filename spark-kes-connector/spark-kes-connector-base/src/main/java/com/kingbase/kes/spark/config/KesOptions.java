// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.kingbase.kes.spark.config;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class KesOptions {

    public static final ConfigOption<String> KES_FENODES = ConfigOptions.name("kes.fenodes").stringType().withoutDefaultValue().withDescription("");

    public static final ConfigOption<String> KES_BENODES = ConfigOptions.name("kes.benodes").stringType().withoutDefaultValue().withDescription("");

    public static final ConfigOption<Integer> KES_QUERY_PORT = ConfigOptions.name("kes.query.port").intType().withoutDefaultValue().withDescription("");

    public static final String KES_DEFAULT_CLUSTER = "default_cluster";

    public static final ConfigOption<String> KES_TABLE_IDENTIFIER = ConfigOptions.name("kes.table.identifier").stringType().withoutDefaultValue().withDescription("");

    public static final ConfigOption<String> KES_READ_FIELDS = ConfigOptions.name("kes.read.fields").stringType().withoutDefaultValue().withDescription("");

    public static final ConfigOption<String> KES_FILTER_QUERY = ConfigOptions.name("kes.filter.query").stringType().withoutDefaultValue().withDescription("");

    public static final ConfigOption<Integer> KES_FILTER_QUERY_IN_MAX_COUNT = ConfigOptions.name("kes.filter.query.in.max.count").intType().defaultValue(10000).withDescription("");

    public static final ConfigOption<String> KES_USER = ConfigOptions.name("kes.user").stringType().withoutDefaultValue().withDescription("");

    // use password to save kes.request.auth.password
    // reuse credentials mask method in spark ExternalCatalogUtils#maskCredentials
    public static final ConfigOption<String> KES_PASSWORD = ConfigOptions.name("kes.password").stringType().withoutDefaultValue().withDescription("");

    public static final String KES_REQUEST_AUTH_USER = "kes.request.auth.user";
    public static final String KES_REQUEST_AUTH_PASSWORD = "kes.request.auth.password";

    public static final ConfigOption<Integer> KES_REQUEST_RETRIES = ConfigOptions.name("kes.request.retries").intType().defaultValue(3).withDescription("");
    public static final ConfigOption<Integer> KES_REQUEST_CONNECT_TIMEOUT_MS = ConfigOptions.name("kes.request.connect.timeout.ms").intType().defaultValue(30 * 1000).withDescription("");
    public static final ConfigOption<Integer> KES_REQUEST_READ_TIMEOUT_MS = ConfigOptions.name("kes.request.read.timeout.ms").intType().defaultValue(30 * 1000).withDescription("");
    public static final ConfigOption<Integer> KES_REQUEST_QUERY_TIMEOUT_S = ConfigOptions.name("kes.request.query.timeout.s").intType().defaultValue(6 * 60 * 60).withDescription("");

    public static final ConfigOption<Integer> KES_TABLET_SIZE = ConfigOptions.name("kes.request.tablet.size").intType().defaultValue(1).withDescription("");

    public static final ConfigOption<Integer> KES_BATCH_SIZE = ConfigOptions.name("kes.batch.size").intType().defaultValue(4064).withDescription("");

    public static final int KES_BATCH_SIZE_MAX = 65535;

    public static final ConfigOption<Long> KES_EXEC_MEM_LIMIT = ConfigOptions.name("kes.exec.mem.limit").longType().defaultValue(8L * 1024 * 1024 * 1024).withDescription("");

    public static final ConfigOption<String> KES_VALUE_READER_CLASS = ConfigOptions.name("kes.value.reader.class").stringType().withoutDefaultValue().withDescription("");

    public static final ConfigOption<Boolean> KES_DESERIALIZE_ARROW_ASYNC = ConfigOptions.name("kes.deserialize.arrow.async").booleanType().defaultValue(false).withDescription("");

    public static final ConfigOption<Integer> KES_DESERIALIZE_QUEUE_SIZE = ConfigOptions.name("kes.deserialize.queue.size").intType().defaultValue(64).withDescription("");

    public static final ConfigOption<String> KES_WRITE_FIELDS = ConfigOptions.name("kes.write.fields").stringType().withoutDefaultValue().withDescription("");

    public static final ConfigOption<Boolean> KES_WRITE_SCHEMA_LESS = ConfigOptions.name("kes.write.schemaless").booleanType().defaultValue(false).withDescription("");

    public static final ConfigOption<Integer> KES_SINK_BATCH_SIZE = ConfigOptions.name("kes.sink.batch.size").intType().defaultValue(500000).withDescription("");

    public static final ConfigOption<Integer> KES_SINK_MAX_RETRIES = ConfigOptions.name("kes.sink.max-retries").intType().defaultValue(0).withDescription("");
    public static final ConfigOption<Integer> KES_SINK_RETRY_INTERVAL_MS = ConfigOptions.name("kes.sink.retry.interval.ms").intType().defaultValue(10000).withDescription("The interval at which the Spark connector tries to load the batch of data again after load fails.");

    public static final ConfigOption<String> KES_MAX_FILTER_RATIO = ConfigOptions.name("kes.max.filter.ratio").stringType().withoutDefaultValue().withDescription("");

    public static final String STREAM_LOAD_PROP_PREFIX = "kes.sink.properties.";
    public static final String PARTIAL_COLUMNS = "partial_columns";
    public static final String GROUP_COMMIT = "group_commit";
    public static final Set<String> VALID_GROUP_MODE =
            new HashSet<>(Arrays.asList("sync_mode", "async_mode", "off_mode"));

    public static final ConfigOption<Integer> KES_SINK_TASK_PARTITION_SIZE = ConfigOptions.name("kes.sink.task.partition.size").intType().withoutDefaultValue().withDescription("");

    /**
     * Set kes sink task partition size. If you set a small coalesce size and you don't have the action operations, this may result in the same parallelism in your computation.
     * To avoid this, you can use repartition operations. This will add a shuffle step, but means the current upstream partitions will be executed in parallel.
     */
    public static final ConfigOption<Boolean> KES_SINK_TASK_USE_REPARTITION = ConfigOptions.name("kes.sink.task.use.repartition").booleanType().defaultValue(false).withDescription("");

    public static final ConfigOption<Integer> KES_SINK_BATCH_INTERVAL_MS = ConfigOptions.name("kes.sink.batch.interval.ms").intType().defaultValue(0).withDescription("");

    public static final ConfigOption<Boolean> KES_SINK_ENABLE_2PC = ConfigOptions.name("kes.sink.enable-2pc").booleanType().defaultValue(false).withDescription("");

    /**
     * pass through json data when sink to kes in streaming mode
     */
    public static final ConfigOption<Boolean> KES_SINK_STREAMING_PASSTHROUGH = ConfigOptions.name("kes.sink.streaming.passthrough").booleanType().defaultValue(false).withDescription("");

    /**
     * txnId commit or abort interval
     */
    public static final ConfigOption<Integer> KES_SINK_TXN_INTERVAL_MS = ConfigOptions.name("kes.sink.txn.interval.ms").intType().defaultValue(50).withDescription("");

    /**
     * txnId commit or abort retry times
     */
    public static final ConfigOption<Integer> KES_SINK_TXN_RETRIES = ConfigOptions.name("kes.sink.txn.retries").intType().defaultValue(3).withDescription("");

    /**
     * Use automatic redirection of fe without explicitly obtaining the be list
     */
    public static final ConfigOption<Boolean> KES_SINK_AUTO_REDIRECT = ConfigOptions.name("kes.sink.auto-redirect").booleanType().defaultValue(true).withDescription("");

    public static final ConfigOption<Boolean> KES_ENABLE_TLS = ConfigOptions.name("kes.enable.tls").booleanType().defaultValue(false).withDescription("Enable one-way TLS for Kes client protocols.");

    public static final ConfigOption<String> KES_TLS_CA_CERTIFICATE_PATH = ConfigOptions.name("kes.tls.ca-certificate-path").stringType().defaultValue("").withDescription("Path to a PEM CA certificate chain. The JVM default truststore is used when empty.");

    public static final ConfigOption<Boolean> KES_TLS_SKIP_HOSTNAME_VERIFICATION = ConfigOptions.name("kes.tls.skip-hostname-verification").booleanType().defaultValue(false).withDescription("Skip TLS hostname verification while retaining CA verification.");

    public static final ConfigOption<String> KES_TLS_EXCLUDED_PROTOCOLS = ConfigOptions.name("kes.tls.excluded-protocols").stringType().defaultValue("").withDescription("Comma-separated protocols excluded from TLS: http, mysql, thrift, arrowflight.");

    public static final ConfigOption<String> LOAD_MODE = ConfigOptions.name("kes.sink.mode").stringType().defaultValue("stream_load").withDescription("Write mode, supports stream_load, copy_into and tvf.");

    public static final ConfigOption<String> READ_MODE = ConfigOptions.name("kes.read.mode").stringType().defaultValue("arrow").withDescription("");

    public static final ConfigOption<String> KES_READ_FLIGHT_SQL_PREFIX = ConfigOptions.name("kes.read.arrow-flight-sql.prefix").stringType().defaultValue("ApplicationName=Spark ArrowFlightSQL Query").withDescription("");

    public static final ConfigOption<Integer> KES_READ_FLIGHT_SQL_PORT = ConfigOptions.name("kes.read.arrow-flight-sql.port").intType().defaultValue(-1).withDescription("");

    public static final ConfigOption<String> KES_SINK_LABEL_PREFIX = ConfigOptions.name("kes.sink.label.prefix").stringType().defaultValue("spark-kes").withDescription("Label prefix used by Kes sink writes.");

    public static final ConfigOption<String> KES_SINK_S3_ENDPOINT = ConfigOptions.name("kes.sink.s3.endpoint").stringType().withoutDefaultValue().withDescription("Endpoint of the S3-compatible object storage.");

    public static final ConfigOption<String> KES_SINK_S3_REGION = ConfigOptions.name("kes.sink.s3.region").stringType().withoutDefaultValue().withDescription("Region of the S3-compatible object storage.");

    public static final ConfigOption<String> KES_SINK_S3_BUCKET = ConfigOptions.name("kes.sink.s3.bucket").stringType().withoutDefaultValue().withDescription("Bucket used to stage files for the S3 TVF.");

    public static final ConfigOption<String> KES_SINK_S3_PREFIX = ConfigOptions.name("kes.sink.s3.prefix").stringType().withoutDefaultValue().withDescription("Object key path prefix used to stage TVF files.");

    public static final ConfigOption<String> KES_SINK_S3_ACCESS_KEY = ConfigOptions.name("kes.sink.s3.access-key").stringType().withoutDefaultValue().withDescription("Access key of the S3-compatible object storage.");

    public static final ConfigOption<String> KES_SINK_S3_SECRET_KEY = ConfigOptions.name("kes.sink.s3.secret-key").stringType().withoutDefaultValue().withDescription("Secret key of the S3-compatible object storage.");

    public static final ConfigOption<Boolean> KES_SINK_S3_PATH_STYLE_ACCESS = ConfigOptions.name("kes.sink.s3.path-style-access").booleanType().defaultValue(false).withDescription("Whether to use path-style access for object storage.");

    public static final ConfigOption<Integer> KES_THRIFT_MAX_MESSAGE_SIZE = ConfigOptions.name("kes.thrift.max.message.size").intType().defaultValue(Integer.MAX_VALUE).withDescription("") ;

    public static final ConfigOption<Boolean> KES_FE_AUTO_FETCH = ConfigOptions.name("kes.fe.auto.fetch").booleanType().defaultValue(false).withDescription("");

    public static final ConfigOption<Boolean> KES_READ_BITMAP_TO_STRING = ConfigOptions.name("kes.read.bitmap-to-string").booleanType().defaultValue(false).withDescription("");

    public static final ConfigOption<Boolean> KES_READ_BITMAP_TO_BASE64 = ConfigOptions.name("kes.read.bitmap-to-base64").booleanType().defaultValue(false).withDescription("");

    public static final ConfigOption<Boolean> KES_READ_ARRAY_NATIVE_TYPE = ConfigOptions.name("kes.read.array.native-type").booleanType().defaultValue(false).withDescription("If true, Kes ARRAY columns are read as Spark ArrayType(StringType). If false (default), they are read as a JSON-encoded String for backward compatibility.");

    public static final ConfigOption<Integer> KES_SINK_NET_BUFFER_SIZE = ConfigOptions.name("kes.sink.net.buffer.size").intType().defaultValue(1024 * 1024).withDescription("");

    public static final ConfigOption<Boolean> KES_SINK_HTTP_UTF8_CHARSET = ConfigOptions.name("kes.sink.http-utf8-charset").booleanType().defaultValue(false).withDescription("");


}
