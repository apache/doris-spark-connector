package org.apache.doris.spark.config

object DorisConfigOptions {

  val DORIS_FENODES = ConfigOption.builder[String]("doris.fenodes").withoutDefaultValue().withDescription("")

  val DORIS_BENODES = ConfigOption.builder[String]("doris.benodes").withoutDefaultValue().withDescription("")

  val DORIS_QUERY_PORT = ConfigOption.builder[Int]("doris.query.port").withoutDefaultValue().withDescription("")

  val DORIS_DEFAULT_CLUSTER = "default_cluster"

  val DORIS_TABLE_IDENTIFIER = ConfigOption.builder[String]("doris.table.identifier").withoutDefaultValue().withDescription("")

  val DORIS_READ_FIELDS = ConfigOption.builder[String]("doris.read.fields").withoutDefaultValue().withDescription("")

  val DORIS_FILTER_QUERY = ConfigOption.builder[String]("doris.filter.query").withoutDefaultValue().withDescription("")

  val DORIS_FILTER_QUERY_IN_MAX_COUNT = ConfigOption.builder[Int]("doris.filter.query.in.max.count").withoutDefaultValue().withDescription("")

  val DORIS_FILTER_QUERY_IN_VALUE_UPPER_LIMIT = 10000

  val DORIS_USER = ConfigOption.builder[String]("doris.user").withoutDefaultValue().withDescription("")

  val DORIS_REQUEST_AUTH_USER = "doris.request.auth.user"
  // use password to save doris.request.auth.password
  // reuse credentials mask method in spark ExternalCatalogUtils#maskCredentials
  val DORIS_PASSWORD = ConfigOption.builder[String]("doris.password").withoutDefaultValue().withDescription("")
  val DORIS_REQUEST_AUTH_PASSWORD = "doris.request.auth.password"

  val DORIS_REQUEST_RETRIES = ConfigOption.builder[Int]("doris.request.retries").defaultValue(3).withDescription("")
  val DORIS_REQUEST_CONNECT_TIMEOUT_MS = ConfigOption.builder[Int]("doris.request.connect.timeout.ms").defaultValue(30 * 1000).withDescription("")
  val DORIS_REQUEST_READ_TIMEOUT_MS = ConfigOption.builder[Int]("doris.request.read.timeout.ms").defaultValue(30 * 1000).withDescription("")
  val DORIS_REQUEST_QUERY_TIMEOUT_S = ConfigOption.builder[Int]("doris.request.query.timeout.s").defaultValue(6 * 60 * 60).withDescription("")
  val DORIS_REQUEST_RETRIES_DEFAULT = 3
  val DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT: Int = 30 * 1000
  val DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT: Int = 30 * 1000
  val DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT: Int = 6 * 60 * 60

  val DORIS_TABLET_SIZE = ConfigOption.builder[Int]("doris.request.tablet.size").defaultValue(1).withDescription("")
  val DORIS_TABLET_SIZE_DEFAULT = 1
  val DORIS_TABLET_SIZE_MIN = 1

  val DORIS_BATCH_SIZE = ConfigOption.builder[Int]("doris.batch.size").defaultValue(4064).withDescription("")
  val DORIS_BATCH_SIZE_DEFAULT = 1024

  val DORIS_EXEC_MEM_LIMIT = ConfigOption.builder[Long]("doris.exec.mem.limit").defaultValue(8L * 1024 * 1024 * 1024).withDescription("")
  val DORIS_EXEC_MEM_LIMIT_DEFAULT: Long = 8L * 1024 * 1024 * 1024

  val DORIS_VALUE_READER_CLASS = ConfigOption.builder[String]("doris.value.reader.class").withoutDefaultValue().withDescription("")

  val DORIS_DESERIALIZE_ARROW_ASYNC = ConfigOption.builder[Boolean]("doris.deserialize.arrow.async").defaultValue(false).withDescription("")
  val DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT = false

  val DORIS_DESERIALIZE_QUEUE_SIZE = ConfigOption.builder[Int]("doris.deserialize.queue.size").defaultValue(64).withDescription("")
  val DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT = 64

  val DORIS_WRITE_FIELDS = ConfigOption.builder[String]("doris.write.fields").withoutDefaultValue().withDescription("")

  val DORIS_SINK_BATCH_SIZE = ConfigOption.builder[Int]("doris.sink.batch.size").defaultValue(100000).withDescription("")
  val SINK_BATCH_SIZE_DEFAULT = 100000

  val DORIS_SINK_MAX_RETRIES = ConfigOption.builder[Int]("doris.sink.max-retries").defaultValue(0).withDescription("")
  val SINK_MAX_RETRIES_DEFAULT = 0

  val DORIS_MAX_FILTER_RATIO = ConfigOption.builder[String]("doris.max.filter.ratio").withoutDefaultValue().withDescription("")

  val STREAM_LOAD_PROP_PREFIX = "doris.sink.properties."

  val DORIS_SINK_TASK_PARTITION_SIZE = ConfigOption.builder[Int]("doris.sink.task.partition.size").withoutDefaultValue().withDescription("")

  /**
   * Set doris sink task partition size. If you set a small coalesce size and you don't have the action operations, this may result in the same parallelism in your computation.
   * To avoid this, you can use repartition operations. This will add a shuffle step, but means the current upstream partitions will be executed in parallel.
   */
  val DORIS_SINK_TASK_USE_REPARTITION = ConfigOption.builder[Boolean]("doris.sink.task.use.repartition").defaultValue(false).withDescription("")

  val DORIS_SINK_TASK_USE_REPARTITION_DEFAULT = false

  val DORIS_SINK_BATCH_INTERVAL_MS = ConfigOption.builder[Int]("doris.sink.batch.interval.ms").defaultValue(50).withDescription("")

  val DORIS_SINK_BATCH_INTERVAL_MS_DEFAULT = 50

  /**
   * set types to ignore, split by comma
   * e.g.
   * "doris.ignore-type"="bitmap,hll"
   */
  val DORIS_IGNORE_TYPE = "doris.ignore-type"

  val DORIS_SINK_ENABLE_2PC = ConfigOption.builder[Boolean]("doris.sink.enable-2pc").defaultValue(false).withDescription("")

  /**
   * pass through json data when sink to doris in streaming mode
   */
  val DORIS_SINK_STREAMING_PASSTHROUGH = "doris.sink.streaming.passthrough"
  val DORIS_SINK_STREAMING_PASSTHROUGH_DEFAULT = false

  /**
   * txnId commit or abort interval
   */
  val DORIS_SINK_TXN_INTERVAL_MS = ConfigOption.builder[Int]("doris.sink.txn.interval.ms").defaultValue(50).withDescription("")
  val DORIS_SINK_TXN_INTERVAL_MS_DEFAULT = 50

  /**
   * txnId commit or abort retry times
   */
  val DORIS_SINK_TXN_RETRIES = ConfigOption.builder[Int]("doris.sink.txn.retries").defaultValue(3).withDescription("")
  val DORIS_SINK_TXN_RETRIES_DEFAULT = 3

  /**
   * Use automatic redirection of fe without explicitly obtaining the be list
   */
  val DORIS_SINK_AUTO_REDIRECT = "doris.sink.auto-redirect"
  val DORIS_SINK_AUTO_REDIRECT_DEFAULT = true

  /**
   * compress_type
   */
  val DORIS_SINK_DATA_COMPRESS_TYPE = "doris.sink.properties.compress_type"

  val DORIS_ENABLE_HTTPS = ConfigOption.builder[Boolean]("doris.enable.https").defaultValue(false).withDescription("")

  val DORIS_ENABLE_HTTPS_DEFAULT = false

  val DORIS_HTTPS_KEY_STORE_PATH = ConfigOption.builder[String]("doris.https.key-store-path").withoutDefaultValue().withDescription("")

  val DORIS_HTTPS_KEY_STORE_TYPE = ConfigOption.builder[String]("doris.https.key-store-type").defaultValue("JKS").withDescription("")

  val DORIS_HTTPS_KEY_STORE_TYPE_DEFAULT = "JKS"

  val DORIS_HTTPS_KEY_STORE_PASSWORD = ConfigOption.builder[String]("doris.https.key-store-password").withoutDefaultValue().withDescription("")

  val LOAD_MODE = ConfigOption.builder[String]("doris.sink.load.mode").defaultValue("stream_load").withDescription("")
  val DEFAULT_LOAD_MODE = "stream_load"

  val READ_MODE = ConfigOption.builder[String]("doris.read.mode").defaultValue("thrift").withDescription("")

  val DORIS_SINK_TAG = ConfigOption.builder[String]("doris.sink.tag").defaultValue(s"spark-${System.currentTimeMillis()}").withDescription("")

  val DORIS_UNSUPPORTED_COLUMNS = ConfigOption.builder[String]("doris.unsupported.columns").defaultValue("").withDescription("")

}
