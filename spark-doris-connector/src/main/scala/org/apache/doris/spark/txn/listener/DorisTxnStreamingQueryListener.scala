package org.apache.doris.spark.txn.listener

import org.apache.doris.spark.cfg.SparkSettings
import org.apache.doris.spark.txn.TransactionHandler
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.util.CollectionAccumulator

import scala.collection.JavaConverters._
import scala.collection.mutable

class DorisTxnStreamingQueryListener(preCommittedTxnAcc: CollectionAccumulator[Long], settings: SparkSettings)
  extends StreamingQueryListener with Logging {

  private val txnHandler = TransactionHandler(settings)

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    // do commit transaction when each batch ends
    val txnIds: mutable.Buffer[Long] = preCommittedTxnAcc.value.asScala
    if (txnIds.isEmpty) {
      log.warn("job run succeed, but there is no pre-committed txn ids")
      return
    }
    log.info(s"batch[${event.progress.batchId}] run succeed, start committing transactions")
    try txnHandler.commitTransactions(txnIds.toList)
    catch {
      case e: Exception => throw e
    } finally preCommittedTxnAcc.reset()
    log.info(s"batch[${event.progress.batchId}] commit transaction success")
  }


  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    val txnIds: mutable.Buffer[Long] = preCommittedTxnAcc.value.asScala
    // if job failed, abort all pre committed transactions
    if (event.exception.nonEmpty) {
      if (txnIds.isEmpty) {
        log.warn("job run failed, but there is no pre-committed txn ids")
        return
      }
      log.info("job run failed, start aborting transactions")
      try txnHandler.abortTransactions(txnIds.toList)
      catch {
        case e: Exception => throw e
      } finally preCommittedTxnAcc.reset()
      log.info("abort transaction success")
    }
  }

}
