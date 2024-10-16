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

package org.apache.doris.spark.client

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.doris.spark.config.{DorisConfig, DorisConfigOptions}
import org.apache.doris.spark.util.{ArrowUtils, RowConvertors}
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.types.{StructField, StructType}

import java.io.ByteArrayOutputStream

class StreamLoadProcessor(config: DorisConfig, var schema: StructType = StructType(Array[StructField]()))
  extends AbstractStreamLoadProcessor[InternalRow](config) {

  override def toArrowFormat(rowArray: Array[InternalRow]): Array[Byte] = {
    val arrowSchema = ArrowUtils.toArrowSchema(schema, "UTC")
    val root = VectorSchemaRoot.create(arrowSchema, new RootAllocator(Integer.MAX_VALUE))
    val arrowWriter = ArrowWriter.create(root)
    rowArray.foreach(arrowWriter.write)
    arrowWriter.finish()
    val out = new ByteArrayOutputStream
    val writer = new ArrowStreamWriter(root, new DictionaryProvider.MapDictionaryProvider, out)
    writer.writeBatch()
    writer.end()
    val arrowBytes = out.toByteArray
    arrowBytes
  }

  override def stringify(row: InternalRow, format: String): String = {
    format match {
      case "csv" => RowConvertors.convertToCsv(row, schema, columnSep)
      case "json" => RowConvertors.convertToJson(row, schema)
    }
  }

  override def getWriteFields: String = {
    if (config.contains(DorisConfigOptions.DORIS_WRITE_FIELDS)) {
      config.getValue(DorisConfigOptions.DORIS_WRITE_FIELDS)
    } else schema.fields.map(f => s"`${f.name}`").mkString(",")
  }

  override protected def generateStreamLoadLabel(): String = {
    val taskContext = TaskContext.get()
    val stageId = taskContext.stageId()
    val taskAttemptId = taskContext.taskAttemptId()
    val partitionId = taskContext.partitionId()
    s"spark-$stageId-$taskAttemptId-$partitionId-${System.currentTimeMillis()}"
  }
}
