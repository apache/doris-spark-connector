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
 * Example to read data from Doris using Spark Batch.
 */
object DorisReadExample {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local[1]").getOrCreate()
    val dorisSparkDF = session.read.format("doris")
      .option("doris.table.identifier", "test.student")
      .option("doris.fenodes", "127.0.0.1:8030")
      .option("user", "root")
      .option("password", "")
      .load()

    dorisSparkDF.show()
  }
}
