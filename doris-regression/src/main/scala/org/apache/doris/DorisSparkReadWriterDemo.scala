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

package org.apache.doris

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DorisSparkReadWriterDemo {

  def main(args: Array[String]): Unit = {
    println(s"Input arguments: ${args.mkString(" ")}")
    val arguments = DorisArguments.parse(args)
    val dorisTlsOptions = arguments.tlsOptions

    val sparkConf: SparkConf = new SparkConf().setMaster("local[1]")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val dorisReader = spark.read
      .format("doris")
      .option("doris.fenodes", arguments.feAddress)
      .option("doris.table.identifier", arguments.readTableIdentifier)
      .option("user", arguments.user)
      .option("password", arguments.password)

    val dorisSparkDF = dorisTlsOptions
      .foldLeft(dorisReader) { case (reader, (name, value)) =>
        reader.option(name, value)
      }
      .load()

    dorisSparkDF.show()

    val dorisWriter = dorisSparkDF.write
      .format("doris")
      .option("doris.fenodes", arguments.feAddress)
      .option("doris.table.identifier", arguments.writeTableIdentifier)
      .option("user", arguments.user)
      .option("password", arguments.password)
      .option("sink.batch.size",3)
      .option("sink.max-retries",2)

    dorisTlsOptions
      .foldLeft(dorisWriter) { case (writer, (name, value)) =>
        writer.option(name, value)
      }
      .mode("append")
      .save()

    spark.stop()
  }
}
