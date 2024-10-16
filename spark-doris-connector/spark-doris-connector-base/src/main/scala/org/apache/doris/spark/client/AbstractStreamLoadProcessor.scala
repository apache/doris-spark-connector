// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.spark.client

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.doris.spark.config.{DorisConfig, DorisConfigOptions}
import org.apache.doris.spark.util.HttpUtils
import org.apache.http.client.entity.GzipCompressingEntity
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPut, HttpRequestBase}
import org.apache.http.entity.{BufferedHttpEntity, InputStreamEntity}
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpEntity, HttpHeaders, HttpStatus}
import org.slf4j.{Logger, LoggerFactory}

import java.io.{PipedInputStream, PipedOutputStream}
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Callable, Executors, Future, ThreadFactory}
import scala.collection.mutable
import scala.reflect.ClassTag

protected[spark] abstract class AbstractStreamLoadProcessor[R:ClassTag](config: DorisConfig) extends DorisWriter[R] with DorisCommitter {

  protected val log: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))

  private val MAPPER = JsonMapper.builder().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false).addModule(DefaultScalaModule).build()

  private lazy val httpClient: CloseableHttpClient = HttpUtils.getHttpClient(config)

  private lazy val (db, table): (String, String) = {
    val tableIdentifier = config.getValue(DorisConfigOptions.DORIS_TABLE_IDENTIFIER)
    val dbTableArr = tableIdentifier.split("\\.")
    (dbTableArr(0).replaceAll("`", "").trim, dbTableArr(1).replaceAll("`", "").trim)
  }

  private val STREAM_LOAD_SUCCESS_STATUS: Array[String] = Array("Success", "Publish Timeout")

  private val twoPhaseCommitEnabled = config.getValue(DorisConfigOptions.DORIS_SINK_ENABLE_2PC)

  private var properties: Map[String, String] = config.getSinkProperties

  private val format = properties.getOrElse("format", "csv")

  protected var columnSep: String = _

  private var lineDelim: String = _

  private val gzipCompressEnabled: Boolean = properties.contains("compress_type") && properties("compress_type") == "gzip"

  private val httpsEnabled: Boolean = config.getValue(DorisConfigOptions.DORIS_ENABLE_HTTPS)

  /**
   * partial_columns
   */
  private val PARTIAL_COLUMNS: String = "partial_columns"

  /**
   * Group commit
   */
  private val GROUP_COMMIT: String = "group_commit"
  private val VALID_GROUP_MODE: Set[String] = Set[String]("sync_mode", "async_mode", "off_mode")

  private val groupCommit: String = if (properties.contains(GROUP_COMMIT)) {
    var msg = ""
    if (twoPhaseCommitEnabled) msg = ""
    if (properties.contains(PARTIAL_COLUMNS) && properties(PARTIAL_COLUMNS).toLowerCase == "true") msg = ""
    if (!VALID_GROUP_MODE.contains(properties(GROUP_COMMIT).toLowerCase())) msg = ""
    if (msg.nonEmpty) throw new IllegalArgumentException(msg)
    properties(GROUP_COMMIT).toLowerCase
  } else null

  private var label: String = _

  private var output: PipedOutputStream = _

  private var createNewBatch = true

  private var isFirstRecordOfBatch = true

  private lazy val recordBuffer = mutable.ListBuffer[R]()

  private val arrowBufferSize = 1000

  private lazy val executor = Executors.newSingleThreadExecutor(new ThreadFactory {

    private val id = new AtomicInteger()

    override def newThread(r: Runnable): Thread = {
      val thread = new Thread(r)
      thread.setName("stream-load-worker-" + id.getAndIncrement())
      thread.setDaemon(true)
      thread
    }
  })

  private var requestFuture: Option[Future[CloseableHttpResponse]] = None

  {
    DorisFrontend.initialize(config)
  }

  def load(row: R): Unit = {

    if (createNewBatch) {
      requestFuture = DorisFrontend.requestFrontends[Future[CloseableHttpResponse]]()(frontEnd => {
        val httpPut = new HttpPut(URLs.streamLoad(frontEnd.host, frontEnd.httpPort, db, table, httpsEnabled))
        handleStreamLoadProperties(httpPut)
        val pipedInputStream = new PipedInputStream(4096)
        output = new PipedOutputStream(pipedInputStream)
        var entity: HttpEntity = new InputStreamEntity(pipedInputStream)
        if (gzipCompressEnabled) {
          entity = new GzipCompressingEntity(entity)
        }
        httpPut.setEntity(entity)
        executor.submit(new Callable[CloseableHttpResponse] {
          override def call(): CloseableHttpResponse = httpClient.execute(httpPut)
        })
      })
      createNewBatch = false
    }
    output.write(toFormat(row, format))

  }

  @throws[Exception]
  override def stop(): String = {
    output.close()
    val res = requestFuture.get.get
    if (res.getStatusLine.getStatusCode != HttpStatus.SC_OK) {
      throw new Exception()
    }
    val resEntity = EntityUtils.toString(new BufferedHttpEntity(res.getEntity))
    val response = MAPPER.readValue(resEntity, classOf[StreamLoadResponse])
    if (STREAM_LOAD_SUCCESS_STATUS.contains(response.Status)) {
      //
      createNewBatch = true
      if (twoPhaseCommitEnabled) response.TxnId.toString else null
    } else {
      log.error(s"stream load execute failed, status: ${response.Status}, msg: ${response.msg}")
      throw new Exception(s"stream load execute failed, status: ${response.Status}, msg: ${response.msg}")
    }
  }

  override def commit(m: String): Unit = {
    DorisFrontend.requestFrontends()(frontEnd => {
      val httpPut = new HttpPut(URLs.streamLoad2PC(frontEnd.host, frontEnd.httpPort, db, httpsEnabled))
      handleCommitHeaders(httpPut, m)
      val response = httpClient.execute(httpPut)
      if (response.getStatusLine.getStatusCode != HttpStatus.SC_OK) {
        throw new Exception()
      }
    })
  }

  override def abort(m: String): Unit = {
    DorisFrontend.requestFrontends()(frontEnd => {
      val httpPut = new HttpPut(URLs.streamLoad2PC(frontEnd.host, frontEnd.httpPort, db, httpsEnabled))
      handleAbortHeaders(httpPut, m)
      val response = httpClient.execute(httpPut)
      if (response.getStatusLine.getStatusCode != HttpStatus.SC_OK) {
        throw new Exception()
      }
    })
  }

  private def toFormat(row: R, format: String): Array[Byte] = {
    format.toLowerCase match {
      case "csv" | "json" => toStringFormat(row, format)
      case "arrow" =>
        recordBuffer += row
        if (recordBuffer.size < arrowBufferSize) {
          Array.empty[Byte]
        } else {
          val dataArray = recordBuffer.toArray
          recordBuffer.clear()
          toArrowFormat(dataArray)
        }
      case format: String => throw new IllegalArgumentException(format)
    }
  }

  private def toStringFormat(row: R, format: String): Array[Byte] = {
    val prefix = if (!isFirstRecordOfBatch) {
      isFirstRecordOfBatch = false
      ""
    } else lineDelim
    val stringRow = stringify(row, format)
    (prefix + stringRow).getBytes(StandardCharsets.UTF_8)
  }

  def stringify(row: R, format: String): String

  def toArrowFormat(rowArray: Array[R]): Array[Byte]

  def getWriteFields: String

  private def handleStreamLoadProperties(httpPut: HttpPut): Unit = {
    addCommonHeaders(httpPut)

    // handle label
    if (groupCommit == null || groupCommit == "off_mode") {
      label = generateStreamLoadLabel()
      httpPut.setHeader("label", label)
    }

    // handle columns
    val writeFields = getWriteFields
    httpPut.setHeader("columns", writeFields)

    // handle filter ratio
    if (config.contains(DorisConfigOptions.DORIS_MAX_FILTER_RATIO)) {
      httpPut.setHeader("max_filter_ratio", config.getValue(DorisConfigOptions.DORIS_MAX_FILTER_RATIO))
    }

    // handle 2pc
    if (twoPhaseCommitEnabled) httpPut.setHeader("two_phase_commit", "true")

    // handle separators
    format.toLowerCase match {
      case "csv" =>
        if (!properties.contains("column_separator")) {
          properties += "column_separator" -> "\t"
        }
        columnSep = properties("column_separator")
        if (!properties.contains("line_delimiter")) {
          properties += "line_delimiter" -> "\n"
        }
        lineDelim = properties("line_delimiter")
      case "json" =>
        if (!properties.contains("line_delimiter")) {
          properties += "line_delimiter" -> "\n"
        }
        lineDelim = properties("line_delimiter")
    }

    properties.foreach { case (k, v) => httpPut.setHeader(k, v) }

  }

  private def handleCommitHeaders(httpPut: HttpPut, txnId: String): Unit = {
    addCommonHeaders(httpPut)
    httpPut.setHeader("txn_operation", "commit")
    httpPut.setHeader("txn_id", txnId)
  }

  private def handleAbortHeaders(httpPut: HttpPut, txnId: String): Unit = {
    addCommonHeaders(httpPut)
    httpPut.setHeader("txn_operation", "abort")
    httpPut.setHeader("txn_id", txnId)
  }

  private def addCommonHeaders(req: HttpRequestBase): Unit = {
    val user = config.getValue(DorisConfigOptions.DORIS_USER)
    val passwd = config.getValue(DorisConfigOptions.DORIS_PASSWORD)
    HttpUtils.setAuth(req, user, passwd)
    req.setHeader(HttpHeaders.EXPECT, "100-continue")
    req.setHeader(HttpHeaders.CONTENT_TYPE, "text/plain; charset=UTF-8")
  }

  protected def generateStreamLoadLabel(): String

  override def close(): Unit = {
    createNewBatch = true
    httpClient.close()
  }

}
