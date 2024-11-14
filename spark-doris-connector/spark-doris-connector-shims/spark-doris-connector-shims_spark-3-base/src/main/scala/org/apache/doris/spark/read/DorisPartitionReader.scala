package org.apache.doris.spark.read

import org.apache.doris.spark.client.read.{DorisReader, DorisThriftReader}
import org.apache.doris.spark.client.DorisReaderPartition
import org.apache.doris.spark.config.DorisConfig
import org.apache.doris.spark.util.RowConvertors
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.types.StructType

import scala.language.implicitConversions

class DorisPartitionReader(inputPartition: InputPartition, schema: StructType, mode: ScanMode, config: DorisConfig)
  extends PartitionReader[InternalRow] {

  private implicit def toReaderPartition(inputPart: DorisInputPartition): DorisReaderPartition =
    DorisReaderPartition(inputPart.database, inputPart.table, inputPart.backend, inputPart.tablets, inputPart.opaquedQueryPlan)

  private lazy val reader: DorisReader = {
    mode match {
      case ScanMode.THRIFT => new DorisThriftReader(inputPartition.asInstanceOf[DorisInputPartition], config)
      case _ => throw new UnsupportedOperationException()
    }
  }

  override def next(): Boolean = reader.hasNext

  override def get(): InternalRow = {
    val values = reader.next().asInstanceOf[Array[Any]]
    if (values.nonEmpty) {
      val row = new SpecificInternalRow(schema.fields.map(_.dataType))
      values.zipWithIndex.foreach {
        case (value, index) =>
          if (value == null) row.setNullAt(index)
          else row.update(index, RowConvertors.convertValue(value, schema.fields(index).dataType))
      }
      row
    } else null.asInstanceOf[InternalRow]
  }

  override def close(): Unit = {
    if (reader != null) {
      reader.close()
    }
  }

}
