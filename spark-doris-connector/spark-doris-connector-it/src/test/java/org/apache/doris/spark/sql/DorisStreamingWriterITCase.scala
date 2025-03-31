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

package org.apache.doris.spark.sql

import org.apache.doris.spark.container.AbstractContainerTestBase.getDorisQueryConnection
import org.apache.doris.spark.container.{AbstractContainerTestBase, ContainerUtils}
import org.apache.doris.spark.rest.models.DataModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryProgress}
import org.junit.{Before, Test}
import org.slf4j.LoggerFactory

/**
 * Doris Structured Streaming Test Case.
 */
class DorisStreamingWriterITCase extends AbstractContainerTestBase {

  private val LOG = LoggerFactory.getLogger(classOf[DorisStreamingWriterITCase])
  val DATABASE = "test_doris_streaming"
  val TABLE_WRITE_TBL_STREAM = "tbl_write_tbl_stream"

  @Before
  def setUp(): Unit = {
    ContainerUtils.executeSQLStatement(getDorisQueryConnection,
      LOG,
      String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE))
  }

  @Test
  def testStreamingWriter(): Unit = {
    initializeTable(TABLE_WRITE_TBL_STREAM, DataModel.DUPLICATE)
    val spark = SparkSession.builder()
      .appName("RateSourceExample")
      .master("local[1]")
      .getOrCreate()

    val rateStream = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load()

    //timestamp,value
    val dorissink =  rateStream.writeStream
      .format("doris")
      .option("checkpointLocation", "/tmp/checkpoint")
      .option("doris.table.identifier", DATABASE + "." + TABLE_WRITE_TBL_STREAM)
      .option("doris.fenodes", getFenodes)
      .option("user", getDorisUsername)
      .option("password", getDorisPassword)
      .start()

    var totalCount = 0L
    spark.streams.addListener(new StreamingQueryListener {
        override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

        override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
          val progress: StreamingQueryProgress = event.progress
          val numInputRows = progress.numInputRows
          totalCount += numInputRows
          println(s"Processed $numInputRows rows, Total: $totalCount")

          if (totalCount >= 10) {
            println(s"Total count reached 10, stopping query.")
            dorissink.stop()
          }
        }

        override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
      })

    dorissink.awaitTermination()
    val cnt = ContainerUtils.executeSQLStatement(
      getDorisQueryConnection(DATABASE),
      LOG,
      String.format("SELECT count(1) FROM %s.%s", DATABASE, TABLE_WRITE_TBL_STREAM),
      1
    )
    assert(cnt.get(0).toInt == totalCount)
  }

  private def initializeTable(table: String, dataModel: DataModel): Unit = {
    val max = if (DataModel.AGGREGATE == dataModel) "MAX" else ""
    val morProps = if (!(DataModel.UNIQUE_MOR == dataModel)) "" else ",\"enable_unique_key_merge_on_write\" = \"false\""
    val model = if (dataModel == DataModel.UNIQUE_MOR) DataModel.UNIQUE.toString else dataModel.toString
    ContainerUtils.executeSQLStatement(
      getDorisQueryConnection,
      LOG,
      String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE),
      String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, table),
      String.format("CREATE TABLE %s.%s ( \n"
        + "`timestamp` DATETIME,\n"
        + "`value` int %s\n"
        + ") "
        + " %s KEY(`timestamp`) "
        + " DISTRIBUTED BY HASH(`timestamp`) BUCKETS 1\n"
        + "PROPERTIES ("
        + "\"replication_num\" = \"1\"\n" + morProps + ")", DATABASE, table, max, model))
  }
}
