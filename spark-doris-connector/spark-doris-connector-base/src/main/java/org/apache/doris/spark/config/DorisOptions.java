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

    public static final ConfigOption<Integer> DORIS_SINK_BATCH_SIZE = ConfigOptions.name("doris.sink.batch.size").intType().defaultValue(500000).withDescription("");

    public static final ConfigOption<Integer> DORIS_SINK_MAX_RETRIES = ConfigOptions.name("doris.sink.max-retries").intType().defaultValue(0).withDescription("");
    public static final ConfigOption<Integer> DORIS_SINK_RETRY_INTERVAL_MS = ConfigOptions.name("doris.sink.retry.interval.ms").intType().defaultValue(10000).withDescription("The interval at which the Spark connector tries to load the batch of data again after load fails.");

    public static final ConfigOption<String> DORIS_MAX_FILTER_RATIO = ConfigOptions.name("doris.max.filter.ratio").stringType().withoutDefaultValue().withDescription("");

    public static final String STREAM_LOAD_PROP_PREFIX = "doris.sink.properties.";

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

    public static final ConfigOption<Boolean> DORIS_ENABLE_HTTPS = ConfigOptions.name("doris.enable.https").booleanType().defaultValue(false).withDescription("");

    public static final ConfigOption<String> DORIS_HTTPS_KEY_STORE_PATH = ConfigOptions.name("doris.https.key-store-path").stringType().withoutDefaultValue().withDescription("");

    public static final ConfigOption<String> DORIS_HTTPS_KEY_STORE_TYPE = ConfigOptions.name("doris.https.key-store-type").stringType().defaultValue("JKS").withDescription("");

    public static final ConfigOption<String> DORIS_HTTPS_KEY_STORE_PASSWORD = ConfigOptions.name("doris.https.key-store-password").stringType().withoutDefaultValue().withDescription("");

    public static final ConfigOption<String> LOAD_MODE = ConfigOptions.name("doris.sink.mode").stringType().defaultValue("stream_load").withDescription("");

    public static final ConfigOption<String> READ_MODE = ConfigOptions.name("doris.read.mode").stringType().defaultValue("thrift").withDescription("");

    public static final ConfigOption<String> DORIS_READ_FLIGHT_SQL_PREFIX = ConfigOptions.name("doris.read.arrow-flight-sql.prefix").stringType().defaultValue("ApplicationName=Spark ArrowFlightSQL Query").withDescription("");

    public static final ConfigOption<Integer> DORIS_READ_FLIGHT_SQL_PORT = ConfigOptions.name("doris.read.arrow-flight-sql.port").intType().withoutDefaultValue().withDescription("");

    public static final ConfigOption<String> DORIS_SINK_LABEL_PREFIX = ConfigOptions.name("doris.sink.label.prefix").stringType().defaultValue("spark-doris").withDescription("");

    public static final ConfigOption<Integer> DORIS_THRIFT_MAX_MESSAGE_SIZE = ConfigOptions.name("doris.thrift.max.message.size").intType().defaultValue(Integer.MAX_VALUE).withDescription("") ;

    public static final ConfigOption<Boolean> DORIS_FE_AUTO_FETCH = ConfigOptions.name("doris.fe.auto.fetch").booleanType().defaultValue(false).withDescription("");

    public static final ConfigOption<Boolean> DORIS_READ_BITMAP_TO_STRING = ConfigOptions.name("doris.read.bitmap-to-string").booleanType().defaultValue(false).withDescription("");

    public static final ConfigOption<Boolean> DORIS_READ_BITMAP_TO_BASE64 = ConfigOptions.name("doris.read.bitmap-to-base64").booleanType().defaultValue(false).withDescription("");

    public static final ConfigOption<Integer> DORIS_SINK_NET_BUFFER_SIZE = ConfigOptions.name("doris.sink.net.buffer.size").intType().defaultValue(1024 * 1024).withDescription("");


}