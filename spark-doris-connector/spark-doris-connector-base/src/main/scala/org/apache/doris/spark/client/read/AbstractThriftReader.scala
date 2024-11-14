package org.apache.doris.spark.client.read

import org.apache.doris.sdk.thrift.{TScanCloseParams, TScanNextBatchParams, TScanOpenParams, TScanOpenResult}
import org.apache.doris.spark.client.{DorisBackend, DorisFrontend, DorisReaderPartition, DorisSchema}
import org.apache.doris.spark.config.{DorisConfig, DorisOptions}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.{Condition, Lock, ReentrantLock}
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, TimeUnit}
import scala.collection.JavaConverters._
import scala.util.control.Breaks

protected[spark] abstract class AbstractThriftReader(partition: DorisReaderPartition, config: DorisConfig)
  extends DorisReader(partition, config) {

  protected val log: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))

  protected lazy val frontend: DorisFrontend = DorisFrontend(config)
  private lazy val backend = new DorisBackend(partition.backend, config)

  private[this] var offset = 0

  private[this] val eos: AtomicBoolean = new AtomicBoolean(false)
  private[this] lazy val async: Boolean = config.getValue(DorisOptions.DORIS_DESERIALIZE_ARROW_ASYNC)

  private[this] val beLock = if (async) new ReentrantLock() else new NoOpLock

  private val scanOpenParams = buildScanOpenParams()
  protected val scanOpenResult: TScanOpenResult = lockClient(_.openScanner(scanOpenParams))
  protected val contextId: String = scanOpenResult.getContextId
  protected val dorisSchema: DorisSchema

  private[this] val rowBatchQueue: BlockingQueue[RowBatch] = {
    if (async) {
      val blockingQueueSize = config.getValue(DorisOptions.DORIS_DESERIALIZE_QUEUE_SIZE)
      new ArrayBlockingQueue(blockingQueueSize)
    } else null
  }

  private[this] val asyncThread: Thread = new Thread {
    override def run(): Unit = {
      val nextBatchParams = new TScanNextBatchParams
      nextBatchParams.setContextId(contextId)
      while (!eos.get) {
        nextBatchParams.setOffset(offset)
        val nextResult = lockClient(_.getNext(nextBatchParams))
        eos.set(nextResult.isEos)
        if (!eos.get) {
          rowBatch = new RowBatch(nextResult, dorisSchema)
          offset += rowBatch.getReadRowCount
          rowBatch.close()
          rowBatchQueue.put(rowBatch)
        }
      }
    }
  }

  private val asyncThreadStarted: Boolean = {
    var started = false
    if (async) {
      asyncThread.start()
      started = true
    }
    started
  }

  override def hasNext: Boolean = {
    var hasNext = false
    if (async && asyncThreadStarted) {
      // support deserialize Arrow to RowBatch asynchronously
      if (rowBatch == null || !rowBatch.hasNext) {
        val loop = new Breaks
        loop.breakable {
          while (!eos.get || !rowBatchQueue.isEmpty) {
            if (!rowBatchQueue.isEmpty) {
              rowBatch = rowBatchQueue.take
              hasNext = true
              loop.break
            } else {
              // wait for rowBatch put in queue or eos change
              Thread.sleep(5)
            }
          }
        }
      } else {
        hasNext = true
      }
    } else {
      // Arrow data was acquired synchronously during the iterative process
      if (!eos.get && (rowBatch == null || !rowBatch.hasNext)) {
        if (rowBatch != null) {
          offset += rowBatch.getReadRowCount
          rowBatch.close()
        }
        val nextBatchParams = new TScanNextBatchParams
        nextBatchParams.setContextId(contextId)
        nextBatchParams.setOffset(offset)
        val nextResult = lockClient(_.getNext(nextBatchParams))
        eos.set(nextResult.isEos)
        if (!eos.get) {
          rowBatch = new RowBatch(nextResult, dorisSchema)
        }
      }
      hasNext = !eos.get
    }
    hasNext
  }

  override def next(): AnyRef = {
    if (!hasNext) {
      throw new Exception()
    }
    rowBatch.next.toArray
  }

  override def close(): Unit = {
    val closeParams = new TScanCloseParams
    closeParams.setContextId(contextId)
    lockClient(_.closeScanner(closeParams))
  }

  private def buildScanOpenParams(): TScanOpenParams = {
    val params = new TScanOpenParams
    params.cluster = DorisOptions.DORIS_DEFAULT_CLUSTER
    params.database = partition.database
    params.table = partition.table
    params.tablet_ids = partition.tablets.map(java.lang.Long.valueOf).toList.asJava
    params.opaqued_query_plan = partition.opaquedQueryPlan

    // max row number of one read batch
    val batchSize = config.getValue(DorisOptions.DORIS_BATCH_SIZE)
    val queryDorisTimeout = config.getValue(DorisOptions.DORIS_REQUEST_QUERY_TIMEOUT_S)
    val execMemLimit = config.getValue(DorisOptions.DORIS_EXEC_MEM_LIMIT)

    params.setBatchSize(batchSize)
    params.setQueryTimeout(queryDorisTimeout)
    params.setMemLimit(execMemLimit)
    params.setUser(config.getValue(DorisOptions.DORIS_USER))
    params.setPasswd(config.getValue(DorisOptions.DORIS_PASSWORD))

    log.debug(s"Open scan params is, " +
      s"cluster: ${params.getCluster}, " +
      s"database: ${params.getDatabase}, " +
      s"table: ${params.getTable}, " +
      s"tabletId: ${params.getTabletIds}, " +
      s"batch size: $batchSize, " +
      s"query timeout: $queryDorisTimeout, " +
      s"execution memory limit: $execMemLimit, " +
      s"user: ${params.getUser}, " +
      s"query plan: ${params.getOpaquedQueryPlan}")
    params
  }

  private def lockClient[T](action: DorisBackend => T): T = {
    beLock.lock()
    try {
      action(backend)
    } finally {
      beLock.unlock()
    }
  }

  private class NoOpLock extends Lock {

    override def lock(): Unit = {}

    override def lockInterruptibly(): Unit = {}

    override def tryLock(): Boolean = true

    override def tryLock(time: Long, unit: TimeUnit): Boolean = true

    override def unlock(): Unit = {}

    override def newCondition(): Condition = {
      throw new UnsupportedOperationException("NoOpLock can't provide a condition")
    }

  }

}
