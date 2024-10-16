package org.apache.doris.spark.read

import org.apache.doris.spark.client.{DorisReaderPartition, ReaderPartitionGenerator}
import org.apache.doris.spark.config.DorisConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import scala.language.implicitConversions

class DorisScan(config: DorisConfig, schema: StructType, filters: Array[Filter]) extends Scan with Batch with Logging {

  private val scanMode = ScanMode.THRIFT

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] =
    ReaderPartitionGenerator.generatePartitions(config, schema, filters).map(toInputPartition)

  override def createReaderFactory(): PartitionReaderFactory = {
    new DorisPartitionReaderFactory(readSchema(), scanMode, config)
  }

  private def toInputPartition(rp: DorisReaderPartition): DorisInputPartition =
    DorisInputPartition(rp.database, rp.table, rp.backend, rp.tablets, rp.opaquedQueryPlan)

}

case class DorisInputPartition(database: String, table: String, backend: String, tablets: Array[Long], opaquedQueryPlan: String) extends InputPartition
