package org.apache.doris.spark.client

import org.apache.doris.sdk.thrift.{TDorisExternalService, TScanBatchResult, TScanCloseParams, TScanNextBatchParams, TScanOpenParams, TScanOpenResult, TStatusCode}
import org.apache.doris.spark.config.{DorisConfig, DorisOptions}
import org.apache.doris.spark.exception.ConnectedFailedException
import org.apache.spark.internal.Logging
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TSocket, TTransport, TTransportException}
import org.apache.thrift.{TConfiguration, TException}

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.util.control.Breaks

class DorisBackend(backend: String, config: DorisConfig) extends AutoCloseable with Logging {

  private val (host, port): (String, Int) = {
    val arr = backend.split(":")
    (arr(0).trim, arr(1).trim.toInt)
  }
  private val retries = config.getValue(DorisOptions.DORIS_REQUEST_RETRIES)
  private val connectTimeout = config.getValue(DorisOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS)
  private val socketTimeout = config.getValue(DorisOptions.DORIS_REQUEST_READ_TIMEOUT_MS)

  private var transport: TTransport = _
  private var thriftClient: TDorisExternalService.Client = _

  private var isConnected: Boolean = false

  def connect(): Unit = {
    var ex: TException = null
    var attempt = 0
    while (!isConnected && attempt < retries) {
      try {
        val factory = new TBinaryProtocol.Factory
        transport = new TSocket(new TConfiguration, host, port, socketTimeout, connectTimeout)
        val protocol = factory.getProtocol(transport)
        thriftClient = new TDorisExternalService.Client(protocol)
        if (!transport.isOpen) {
          transport.open()
          isConnected = true
        }
      } catch {
        case e: TTransportException =>
          ex = e
      }
      if (isConnected) {
        return
      }
      attempt += 1
    }
    if (!isConnected) {
      throw new ConnectedFailedException(host + ":" + port, ex)
    }
  }

  def openScanner(openParams: TScanOpenParams): TScanOpenResult = {
    if (log.isDebugEnabled) {
      val logParams = new TScanOpenParams(openParams)
      logParams.setPasswd("********")
      log.debug(s"OpenScanner to '$host:$port', parameter is '$logParams'.")
    }
    if (!isConnected) connect()
    var ex: TException = null
    for (attempt <- 0 until retries) {
      log.debug(s"Attempt $attempt to openScanner $host:$port.")
      try {
        val result = thriftClient.openScanner(openParams)
        Option(result) match {
          case None => log.warn(s"Open scanner result from $host:$port is null.")
          case Some(_) if result.getStatus.getStatusCode != TStatusCode.OK =>
            log.warn(s"The status of open scanner result from $host:$port is '${result.getStatus.getStatusCode}', " +
              s"error message is: ${result.getStatus.getErrorMsgs}.")
          case _ => return result
        }
      } catch {
        case e: TException =>
          log.warn(s"Open scanner from $host:$port failed.", e)
          ex = e
      }
    }
    log.error(host)
    throw new ConnectedFailedException(host, ex)
  }

  def getNext(nextBatchParams: TScanNextBatchParams): TScanBatchResult = {
    log.debug(s"GetNext to '$host:$port', parameter is '$nextBatchParams'.")
    if (!isConnected) connect()
    var ex: TException = null
    var result: TScanBatchResult = null
    for (attempt <- 0 until retries) {
      log.debug(s"Attempt $attempt to getNext $host:$port.")
      try {
        result = thriftClient.getNext(nextBatchParams)
        Option(result) match {
          case None => log.warn(s"GetNext result from $host:$port is null.")
          case Some(_) if result.getStatus.getStatusCode != TStatusCode.OK =>
            log.warn(s"The status of get next result from $host:$port is '${result.getStatus.getStatusCode}', " +
              s"error message is: ${result.getStatus.getErrorMsgs}.")
          case _ => return result
        }
      } catch {
        case e: TException =>
          log.warn(s"Get next from $host:$port failed.", e)
          ex = e
      }
    }
    if (result != null && (TStatusCode.OK ne (result.getStatus.getStatusCode))) {
      val msg = s"$host:$port, ${result.getStatus.getStatusCode}, ${result.getStatus.getErrorMsgs.asScala.mkString("\n")}"
      throw new Exception(msg)
    }
    throw new ConnectedFailedException(host + ":" + port, ex)
  }

  def closeScanner(closeParams: TScanCloseParams): Unit = {
    log.debug(s"CloseScanner to '$host:$port', parameter is '$closeParams.")
    if (!isConnected) try connect()
    catch {
      case e: Exception =>
        log.warn(s"Cannot connect to Doris BE $host:$port when close scanner.", e)
        return
    }
    Breaks.breakable {
      for (attempt <- 0 until retries) {
        log.debug(s"Attempt $attempt to closeScanner $host:$port.")
        try {
          val result = thriftClient.closeScanner(closeParams)
          Option(result) match {
            case None => log.warn(s"CloseScanner result from $host:$port is null.")
            case Some(_) if result.getStatus.getStatusCode != TStatusCode.OK =>
              log.warn(s"The status of get next result from $host:$port is '${result.getStatus.getStatusCode}', " +
                s"error message is: ${result.getStatus.getErrorMsgs}.")
            case _ => Breaks.break()
          }
        } catch {
          case e: TException =>
            log.warn(s"Close scanner from $host:$port failed.", e)
        }
      }
    }
    log.info(s"CloseScanner to Doris BE '$host:$port' success.")
    close()
  }

  override def close(): Unit = {
    log.trace(s"Connect status before close with '$host:$port' is '$isConnected'.")
    isConnected = false
    if (null != thriftClient) thriftClient = null
    if ((transport != null) && transport.isOpen) {
      transport.close()
      log.info(s"Closed a connection to $host:$port.")
    }
  }

}
