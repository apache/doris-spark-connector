package org.apache.doris.spark.write

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.Sink

class DorisStreamSink extends Sink with Serializable {

  override def addBatch(batchId: Long, data: DataFrame): Unit = {

  }
}
