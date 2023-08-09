package org.apache.doris.spark.writer

import org.apache.doris.spark.cfg.SparkSettings

class DorisStreamLoader(settings: SparkSettings) {

  def start(): Unit = Nil

  def load(rowData: Array[Byte]): Unit = {


  }

  def stop(): Unit = Nil

  def commit(): Unit = Nil

  def abort(txnId: Long): Unit = Nil

}
