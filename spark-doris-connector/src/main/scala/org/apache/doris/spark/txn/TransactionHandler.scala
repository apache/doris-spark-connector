package org.apache.doris.spark.txn

import org.apache.doris.spark.cfg.{ConfigurationOptions, SparkSettings}
import org.apache.doris.spark.load.{CachedDorisStreamLoadClient, DorisStreamLoad}
import org.apache.doris.spark.sql.Utils
import org.apache.spark.internal.Logging

import java.time.Duration
import scala.collection.mutable
import scala.util.{Failure, Success}

/**
 * Stream load transaction handler
 *
 * @param settings job settings
 */
class TransactionHandler(settings: SparkSettings) extends Logging {

  private val sinkTxnIntervalMs: Int = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_TXN_INTERVAL_MS,
    ConfigurationOptions.DORIS_SINK_TXN_INTERVAL_MS_DEFAULT)
  private val sinkTxnRetries: Integer = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_TXN_RETRIES,
    ConfigurationOptions.DORIS_SINK_TXN_RETRIES_DEFAULT)
  private val dorisStreamLoad: DorisStreamLoad = CachedDorisStreamLoadClient.getOrCreate(settings)

  /**
   * commit transactions
   *
   * @param txnIds transaction id list
   */
  def commitTransactions(txnIds: List[Long]): Unit = {
    log.debug(s"start to commit transactions, count ${txnIds.size}")
    val (failedTxnIds, ex) = txnIds.map(commitTransaction).filter(_._1.nonEmpty)
      .map(e => (e._1.get, e._2.get))
      .aggregate((mutable.Buffer[Long](), new Exception))(
        (z, r) => ((z._1 += r._1).asInstanceOf[mutable.Buffer[Long]], r._2), (r1, r2) => (r1._1 ++ r2._1, r2._2))
    if (failedTxnIds.nonEmpty) {
      log.error("uncommitted txn ids: {}", failedTxnIds.mkString("[", ",", "]"))
      throw ex
    }
  }

  /**
   * commit single transaction
   *
   * @param txnId transaction id
   * @return
   */
  private def commitTransaction(txnId: Long): (Option[Long], Option[Exception]) = {
    Utils.retry(sinkTxnRetries, Duration.ofMillis(sinkTxnIntervalMs), log) {
      dorisStreamLoad.commit(txnId)
    }() match {
      case Success(_) => (None, None)
      case Failure(e: Exception) => (Option(txnId), Option(e))
    }
  }

  /**
   * abort transactions
   *
   * @param txnIds transaction id list
   */
  def abortTransactions(txnIds: List[Long]): Unit = {
    log.debug(s"start to abort transactions, count ${txnIds.size}")
    var ex: Option[Exception] = None
    val failedTxnIds = txnIds.map(txnId =>
      Utils.retry(sinkTxnRetries, Duration.ofMillis(sinkTxnIntervalMs), log) {
        dorisStreamLoad.abortById(txnId)
      }() match {
        case Success(_) => None
        case Failure(e: Exception) =>
          ex = Option(e)
          Option(txnId)
      }).filter(_.nonEmpty).map(_.get)
    if (failedTxnIds.nonEmpty) {
      log.error("not aborted txn ids: {}", failedTxnIds.mkString("[", ",", "]"))
    }
  }

}

object TransactionHandler {
  def apply(settings: SparkSettings): TransactionHandler = new TransactionHandler(settings)
}
