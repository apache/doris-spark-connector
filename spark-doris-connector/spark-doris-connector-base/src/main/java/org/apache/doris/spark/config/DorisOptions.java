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

package org.apache.doris.spark.config;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class DorisOptions {

    public static final ConfigOption<String> DORIS_FENODES = ConfigOptions.name("doris.fenodes").stringType().withoutDefaultValue().withDescription("");

    public static final ConfigOption<String> DORIS_BENODES = ConfigOptions.name("doris.benodes").stringType().withoutDefaultValue().withDescription("");

    public static final ConfigOption<Integer> DORIS_QUERY_PORT = ConfigOptions.name("doris.query.port").intType().withoutDefaultValue().withDescription("");

    public static final String DORIS_DEFAULT_CLUSTER = "default_cluster";

    public static final ConfigOption<String> DORIS_TABLE_IDENTIFIER = ConfigOptions.name("doris.table.identifier").stringType().withoutDefaultValue().withDescription("");

    public static final ConfigOption<String> DORIS_READ_FIELDS = ConfigOptions.name("doris.read.fields").stringType().withoutDefaultValue().withDescription("");

    public static final ConfigOption<String> DORIS_FILTER_QUERY = ConfigOptions.name("doris.filter.query").stringType().withoutDefaultValue().withDescription("");

    public static final ConfigOption<Integer> DORIS_FILTER_QUERY_IN_MAX_COUNT = ConfigOptions.name("doris.filter.query.in.max.count").intType().defaultValue(10000).withDescription("");

    public static final ConfigOption<String> DORIS_USER = ConfigOptions.name("doris.user").stringType().withoutDefaultValue().withDescription("");

    // use password to save doris.request.auth.password
    // reuse credentials mask method in spark ExternalCatalogUtils#maskCredentials
    public static final ConfigOption<String> DORIS_PASSWORD = ConfigOptions.name("doris.password").stringType().withoutDefaultValue().withDescription("");

    public static final String DORIS_REQUEST_AUTH_USER = "doris.request.auth.user";
    public static final String DORIS_REQUEST_AUTH_PASSWORD = "doris.request.auth.password";

    public static final ConfigOption<Integer> DORIS_REQUEST_RETRIES = ConfigOptions.name("doris.request.retries").intType().defaultValue(3).withDescription("");
    public static final ConfigOption<Integer> DORIS_REQUEST_CONNECT_TIMEOUT_MS = ConfigOptions.name("doris.request.connect.timeout.ms").intType().defaultValue(30 * 1000).withDescription("");
    public static final ConfigOption<Integer> DORIS_REQUEST_READ_TIMEOUT_MS = ConfigOptions.name("doris.request.read.timeout.ms").intType().defaultValue(30 * 1000).withDescription("");
    public static final ConfigOption<Integer> DORIS_REQUEST_QUERY_TIMEOUT_S = ConfigOptions.name("doris.request.query.timeout.s").intType().defaultValue(6 * 60 * 60).withDescription("");

    public static final ConfigOption<Integer> DORIS_TABLET_SIZE = ConfigOptions.name("doris.request.tablet.size").intType().defaultValue(1).withDescription("");

    public static final ConfigOption<Integer> DORIS_BATCH_SIZE = ConfigOptions.name("doris.batch.size").intType().defaultValue(4064).withDescription("");

    public static final int DORIS_BATCH_SIZE_MAX = 65535;

    public static final ConfigOption<Long> DORIS_EXEC_MEM_LIMIT = ConfigOptions.name("doris.exec.mem.limit").longType().defaultValue(8L * 1024 * 1024 * 1024).withDescription("");

    public static final ConfigOption<String> DORIS_VALUE_READER_CLASS = ConfigOptions.name("doris.value.reader.class").stringType().withoutDefaultValue().withDescription("");

    public static final ConfigOption<Boolean> DORIS_DESERIALIZE_ARROW_ASYNC = ConfigOptions.name("doris.deserialize.arrow.async").booleanType().defaultValue(false).withDescription("");

    public static final ConfigOption<Integer> DORIS_DESERIALIZE_QUEUE_SIZE = ConfigOptions.name("doris.deserialize.queue.size").intType().defaultValue(64).withDescription("");

    public static final ConfigOption<String> DORIS_WRITE_FIELDS = ConfigOptions.name("doris.write.fields").stringType().withoutDefaultValue().withDescription("");

    public static final ConfigOption<Boolean> DORIS_WRITE_SCHEMA_LESS = ConfigOptions.name("doris.write.schemaless").booleanType().defaultValue(false).withDescription("");

    public static final ConfigOption<Integer> DORIS_SINK_BATCH_SIZE = ConfigOptions.name("doris.sink.batch.size").intType().defaultValue(500000).withDescription("");

    public static final ConfigOption<Integer> DORIS_SINK_MAX_RETRIES = ConfigOptions.name("doris.sink.max-retries").intType().defaultValue(0).withDescription("");
    public static final ConfigOption<Integer> DORIS_SINK_RETRY_INTERVAL_MS = ConfigOptions.name("doris.sink.retry.interval.ms").intType().defaultValue(10000).withDescription("The interval at which the Spark connector tries to load the batch of data again after load fails.");

    public static final ConfigOption<String> DORIS_MAX_FILTER_RATIO = ConfigOptions.name("doris.max.filter.ratio").stringType().withoutDefaultValue().withDescription("");

    public static final String STREAM_LOAD_PROP_PREFIX = "doris.sink.properties.";
    public static final String PARTIAL_COLUMNS = "partial_columns";
    public static final String GROUP_COMMIT = "group_commit";
    public static final Set<String> VALID_GROUP_MODE =
            new HashSet<>(Arrays.asList("sync_mode", "async_mode", "off_mode"));

    public static final ConfigOption<Integer> DORIS_SINK_TASK_PARTITION_SIZE = ConfigOptions.name("doris.sink.task.partition.size").intType().withoutDefaultValue().withDescription("");

    /**
     * Set doris sink task partition size. If you set a small coalesce size and you don't have the action operations, this may result in the same parallelism in your computation.
     * To avoid this, you can use repartition operations. This will add a shuffle step, but means the current upstream partitions will be executed in parallel.
     */
    public static final ConfigOption<Boolean> DORIS_SINK_TASK_USE_REPARTITION = ConfigOptions.name("doris.sink.task.use.repartition").booleanType().defaultValue(false).withDescription("");

    public static final ConfigOption<Integer> DORIS_SINK_BATCH_INTERVAL_MS = ConfigOptions.name("doris.sink.batch.interval.ms").intType().defaultValue(0).withDescription("");

    public static final ConfigOption<Boolean> DORIS_SINK_ENABLE_2PC = ConfigOptions.name("doris.sink.enable-2pc").booleanType().defaultValue(false).withDescription("");

    /**
     * pass through json data when sink to doris in streaming mode
     */
    public static final ConfigOption<Boolean> DORIS_SINK_STREAMING_PASSTHROUGH = ConfigOptions.name("doris.sink.streaming.passthrough").booleanType().defaultValue(false).withDescription("");

    /**
     * txnId commit or abort interval
     */
    public static final ConfigOption<Integer> DORIS_SINK_TXN_INTERVAL_MS = ConfigOptions.name("doris.sink.txn.interval.ms").intType().defaultValue(50).withDescription("");

    /**
     * txnId commit or abort retry times
     */
    public static final ConfigOption<Integer> DORIS_SINK_TXN_RETRIES = ConfigOptions.name("doris.sink.txn.retries").intType().defaultValue(3).withDescription("");

    /**
     * Use automatic redirection of fe without explicitly obtaining the be list
     */
    public static final ConfigOption<Boolean> DORIS_SINK_AUTO_REDIRECT = ConfigOptions.name("doris.sink.auto-redirect").booleanType().defaultValue(true).withDescription("");

    public static final ConfigOption<Boolean> DORIS_ENABLE_TLS = ConfigOptions.name("doris.enable.tls").booleanType().defaultValue(false).withDescription("Enable one-way TLS for Doris client protocols.");

    public static final ConfigOption<String> DORIS_TLS_CA_CERTIFICATE_PATH = ConfigOptions.name("doris.tls.ca-certificate-path").stringType().defaultValue("").withDescription("Path to a PEM CA certificate chain. The JVM default truststore is used when empty.");

    public static final ConfigOption<Boolean> DORIS_TLS_SKIP_HOSTNAME_VERIFICATION = ConfigOptions.name("doris.tls.skip-hostname-verification").booleanType().defaultValue(false).withDescription("Skip TLS hostname verification while retaining CA verification.");

    public static final ConfigOption<String> DORIS_TLS_EXCLUDED_PROTOCOLS = ConfigOptions.name("doris.tls.excluded-protocols").stringType().defaultValue("").withDescription("Comma-separated protocols excluded from TLS: http, mysql, thrift, arrowflight.");

    public static final ConfigOption<String> LOAD_MODE = ConfigOptions.name("doris.sink.mode").stringType().defaultValue("stream_load").withDescription("Write mode, supports stream_load, copy_into and tvf.");

    public static final ConfigOption<String> READ_MODE = ConfigOptions.name("doris.read.mode").stringType().defaultValue("thrift").withDescription("");

    public static final ConfigOption<String> DORIS_READ_FLIGHT_SQL_PREFIX = ConfigOptions.name("doris.read.arrow-flight-sql.prefix").stringType().defaultValue("ApplicationName=Spark ArrowFlightSQL Query").withDescription("");

    public static final ConfigOption<Integer> DORIS_READ_FLIGHT_SQL_PORT = ConfigOptions.name("doris.read.arrow-flight-sql.port").intType().withoutDefaultValue().withDescription("");

    public static final ConfigOption<String> DORIS_SINK_LABEL_PREFIX = ConfigOptions.name("doris.sink.label.prefix").stringType().defaultValue("spark-doris").withDescription("Label prefix used by Doris sink writes.");

    public static final ConfigOption<String> DORIS_SINK_S3_ENDPOINT = ConfigOptions.name("doris.sink.s3.endpoint").stringType().withoutDefaultValue().withDescription("Endpoint of the S3-compatible object storage.");

    public static final ConfigOption<String> DORIS_SINK_S3_REGION = ConfigOptions.name("doris.sink.s3.region").stringType().withoutDefaultValue().withDescription("Region of the S3-compatible object storage.");

    public static final ConfigOption<String> DORIS_SINK_S3_BUCKET = ConfigOptions.name("doris.sink.s3.bucket").stringType().withoutDefaultValue().withDescription("Bucket used to stage files for the S3 TVF.");

    public static final ConfigOption<String> DORIS_SINK_S3_PREFIX = ConfigOptions.name("doris.sink.s3.prefix").stringType().withoutDefaultValue().withDescription("Object key path prefix used to stage TVF files.");

    public static final ConfigOption<String> DORIS_SINK_S3_ACCESS_KEY = ConfigOptions.name("doris.sink.s3.access-key").stringType().withoutDefaultValue().withDescription("Access key of the S3-compatible object storage.");

    public static final ConfigOption<String> DORIS_SINK_S3_SECRET_KEY = ConfigOptions.name("doris.sink.s3.secret-key").stringType().withoutDefaultValue().withDescription("Secret key of the S3-compatible object storage.");

    public static final ConfigOption<Boolean> DORIS_SINK_S3_PATH_STYLE_ACCESS = ConfigOptions.name("doris.sink.s3.path-style-access").booleanType().defaultValue(false).withDescription("Whether to use path-style access for object storage.");

    public static final ConfigOption<Integer> DORIS_THRIFT_MAX_MESSAGE_SIZE = ConfigOptions.name("doris.thrift.max.message.size").intType().defaultValue(Integer.MAX_VALUE).withDescription("") ;

    public static final ConfigOption<Boolean> DORIS_FE_AUTO_FETCH = ConfigOptions.name("doris.fe.auto.fetch").booleanType().defaultValue(false).withDescription("");

    public static final ConfigOption<Boolean> DORIS_READ_BITMAP_TO_STRING = ConfigOptions.name("doris.read.bitmap-to-string").booleanType().defaultValue(false).withDescription("");

    public static final ConfigOption<Boolean> DORIS_READ_BITMAP_TO_BASE64 = ConfigOptions.name("doris.read.bitmap-to-base64").booleanType().defaultValue(false).withDescription("");

    public static final ConfigOption<Boolean> DORIS_READ_ARRAY_NATIVE_TYPE = ConfigOptions.name("doris.read.array.native-type").booleanType().defaultValue(false).withDescription("If true, Doris ARRAY columns are read as Spark ArrayType(StringType). If false (default), they are read as a JSON-encoded String for backward compatibility.");

    public static final ConfigOption<Integer> DORIS_SINK_NET_BUFFER_SIZE = ConfigOptions.name("doris.sink.net.buffer.size").intType().defaultValue(1024 * 1024).withDescription("");

    public static final ConfigOption<Boolean> DORIS_SINK_HTTP_UTF8_CHARSET = ConfigOptions.name("doris.sink.http-utf8-charset").booleanType().defaultValue(false).withDescription("");


}
