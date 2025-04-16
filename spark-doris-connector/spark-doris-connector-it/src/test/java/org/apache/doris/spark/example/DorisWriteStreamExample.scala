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

package org.apache.doris.spark.example

import org.apache.spark.sql.SparkSession

/**
 * Example to write data to Doris using Spark Structured Streaming.
 */
object DorisWriteStreamExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RateSourceExample")
      .master("local[1]")
      .getOrCreate()

    val rateStream = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 10)
      .load()

    /**
     * root
     * |-- timestamp: timestamp (nullable = true)
     * |-- value: long (nullable = true)
     */
    rateStream.printSchema();

    rateStream.writeStream
      .format("doris")
      .option("checkpointLocation", "/tmp/checkpoint")
      .option("doris.table.identifier", "test_doris_streaming.tbl_write_tbl_stream")
      .option("doris.fenodes", "127.0.0.1:8030")
      .option("user", "root")
      .option("password", "")
      .start()
      .awaitTermination()

    spark.stop();
  }

}
