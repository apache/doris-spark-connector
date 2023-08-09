package org.apache.doris.spark.writer

import org.apache.doris.spark.cfg.{ConfigurationOptions, SparkSettings}
import org.apache.doris.spark.listener.DorisTransactionListener
import org.apache.doris.spark.sql.Utils
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

import java.io.IOException
import java.time.Duration
import java.util
import java.util.Objects
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class DorisWriterV2(settings: SparkSettings) {

  private val logger: Logger = LoggerFactory.getLogger(classOf[DorisWriterV2])

  val batchSize: Int = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_BATCH_SIZE,
    ConfigurationOptions.SINK_BATCH_SIZE_DEFAULT)

  private val enable2PC: Boolean = settings.getBooleanProperty(ConfigurationOptions.DORIS_SINK_ENABLE_2PC,
    ConfigurationOptions.DORIS_SINK_ENABLE_2PC_DEFAULT);

  private val dorisStreamLoader = new DorisStreamLoader(settings)

  def write(dataFrame: DataFrame): Unit = {

    val sc = dataFrame.sqlContext.sparkContext
    val preCommittedTxnAcc = sc.collectionAccumulator[Int]("preCommittedTxnAcc")
    if (enable2PC) {
      sc.addSparkListener(new DorisTransactionListener(preCommittedTxnAcc, dorisStreamLoader))
    }


    val rowSerializer = RowSerializer(settings, dataFrame.columns)

    dataFrame.rdd
      .map(rowSerializer.serialize)
      .foreachPartition(flush)

    /**
     * flush data to Doris and do retry when flush error
     *
     */
    def flush(rowIter: Iterator[Array[Byte]]): Unit = {

      Try {
        dorisStreamLoader.start()
        rowIter.foreach(dorisStreamLoader.load)
      } match {
        case Success(txnIds) => {
          dorisStreamLoader.stop()
          if (enable2PC) txnIds.forEach(txnId => preCommittedTxnAcc.add(txnId))
        }
        case Failure(e) =>
      }

      Try(dorisStreamLoader.load(rowData)) match {
        case Success(txnIds) => if (enable2PC) txnIds.forEach(txnId => preCommittedTxnAcc.add(txnId))
        case Failure(e) =>
          if (enable2PC) {
            // if task run failed, acc value will not be returned to driver,
            // should abort all pre committed transactions inside the task
            logger.info("load task failed, start aborting previously pre-committed transactions")
            val abortFailedTxnIds = mutable.Buffer[Int]()
            preCommittedTxnAcc.value.forEach(txnId => {
              Utils.retry[Unit, Exception](3, Duration.ofSeconds(1), logger) {
                dorisStreamLoader.abort(txnId)
              } match {
                case Success(_) =>
                case Failure(_) => abortFailedTxnIds += txnId
              }
            })
            if (abortFailedTxnIds.nonEmpty) logger.warn("not aborted txn ids: {}", abortFailedTxnIds.mkString(","))
            preCommittedTxnAcc.reset()
          }
          throw new IOException(
            s"Failed to load batch data on BE: ${dorisStreamLoader.getLoadUrlStr} node and exceeded the max ${maxRetryTimes} retry times.", e)
      }
    }

  }

}
