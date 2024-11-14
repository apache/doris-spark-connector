package org.apache.doris.spark.client

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.doris.spark.config.DorisConfig

case class Frontend(host: String, httpPort: Int, queryPort: Int = -1, flightSqlPort: Int = -1)

case class Field(name: String, `type`: String, comment: String,
                 @JsonProperty(value = "aggregation_type") aggregationType: String, precision: Int = 0, scale: Int = 0)

case class DorisSchema(status: Int, keysType: String, properties: List[Field])

case class Tablet(routings: List[String], version: Int, versionHash: Long, schemaHash: Long)

case class QueryPlan(status: Int, @JsonProperty(value = "opaqued_query_plan") opaquedQueryPlan: String, partitions: Map[String, Tablet])

case class StreamLoadResponse(@JsonProperty(value = "TxnId") TxnId: Long,
                              @JsonProperty(value = "msg") msg: String,
                              @JsonProperty(value = "Label") Label: String,
                              @JsonProperty(value = "Status") Status: String,
                              @JsonProperty(value = "ExistingJobStatus") ExistingJobStatus: String,
                              @JsonProperty(value = "Message") Message: String,
                              @JsonProperty(value = "NumberTotalRows") NumberTotalRows: Long,
                              @JsonProperty(value = "NumberLoadedRows") NumberLoadedRows: Long,
                              @JsonProperty(value = "NumberFilteredRows") NumberFilteredRows: Int,
                              @JsonProperty(value = "NumberUnselectedRows") NumberUnselectedRows: Int,
                              @JsonProperty(value = "LoadBytes") LoadBytes: Long,
                              @JsonProperty(value = "LoadTimeMs") LoadTimeMs: Int,
                              @JsonProperty(value = "BeginTxnTimeMs") BeginTxnTimeMs: Int,
                              @JsonProperty(value = "StreamLoadPutTimeMs") StreamLoadPutTimeMs: Int,
                              @JsonProperty(value = "ReadDataTimeMs") ReadDataTimeMs: Int,
                              @JsonProperty(value = "WriteDataTimeMs") WriteDataTimeMs: Int,
                              @JsonProperty(value = "CommitAndPublishTimeMs") CommitAndPublishTimeMs: Int,
                              @JsonProperty(value = "ErrorURL") ErrorURL: String)

case class DorisReaderPartition(database: String, table: String, backend: String, tablets: Array[Long], opaquedQueryPlan: String, config: DorisConfig)
