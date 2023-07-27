package org.apache.doris.spark.listener

import org.apache.doris.spark.load.DorisStreamLoad
import org.apache.doris.spark.sql.Utils
import org.apache.spark.scheduler._
import org.apache.spark.util.CollectionAccumulator
import org.slf4j.{Logger, LoggerFactory}

import java.time.Duration
import scala.collection.mutable
import scala.util.{Failure, Success}

class DorisTransactionListener(acc: CollectionAccumulator[Int], dorisStreamLoad: DorisStreamLoad)
  extends SparkListener {

  val logger: Logger = LoggerFactory.getLogger(classOf[DorisTransactionListener])

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val txnIds = acc.value
    val failedTxnIds = mutable.Buffer[Int]()
    jobEnd.jobResult match {
      // if job succeed, commit all transactions
      case JobSucceeded =>
        logger.info("job run succeed, start commit transactions")
        txnIds.forEach(txnId =>
          Utils.retry(3, Duration.ofSeconds(1), logger) {
            dorisStreamLoad.commit(txnId)
          } match {
            case Success(_) =>
            case Failure(exception) =>
              failedTxnIds += txnId
              logger.error("commit transaction failed, exception {}", exception.getMessage)
          })
        if (failedTxnIds.nonEmpty) {
          logger.error("uncommitted txn ids: {}", failedTxnIds.mkString(","))
        } else {
          logger.info("commit transaction succeed")
        }
      // if job failed, abort all pre committed transactions
      case _ =>
        logger.info("job run failed, start commit transactions")
        txnIds.forEach(txnId =>
          Utils.retry(3, Duration.ofSeconds(1), logger) {
            dorisStreamLoad.abort(txnId)
          } match {
            case Success(_) =>
            case Failure(exception) =>
              failedTxnIds += txnId
              logger.error("abort transaction failed, exception {}", exception.getMessage)
          })
        if (failedTxnIds.nonEmpty) {
          logger.error("not aborted txn ids: {}", failedTxnIds.mkString(","))
        } else {
          logger.info("aborted transaction succeed")
        }
    }
  }

}
