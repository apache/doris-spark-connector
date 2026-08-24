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

package org.apache.doris.spark.util

import org.apache.doris.spark.config.{DorisConfig, DorisOptions}
import org.apache.http.HttpHeaders
import org.apache.http.client.methods.HttpRequestBase
import org.apache.http.config.ConnectionConfig
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.protocol.HttpRequestExecutor

import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.util.Base64

object HttpUtils {

  def getHttpClient(config: DorisConfig): CloseableHttpClient = {

    var connectionConfig = ConnectionConfig.DEFAULT;
    if (config.getValue(DorisOptions.DORIS_SINK_HTTP_UTF8_CHARSET)) {
      connectionConfig = ConnectionConfig.custom()
        .setCharset(StandardCharsets.UTF_8)
        .setMalformedInputAction(CodingErrorAction.REPLACE)
        .setUnmappableInputAction(CodingErrorAction.REPLACE).build()
    }
    val builder = HttpClients.custom()
      .setDefaultConnectionConfig(connectionConfig)
      .setRequestExecutor(new HttpRequestExecutor(60000))
    DorisHttpClientFactory.configure(builder, config.getTlsOptions).build()
  }

  def setAuth(req: HttpRequestBase, user: String, passwd: String): Unit = {
    val authInfo = s"Basic ${Base64.getEncoder.encodeToString((user + ":" + passwd).getBytes(StandardCharsets.UTF_8))}"
    req.setHeader(HttpHeaders.AUTHORIZATION, authInfo)
  }

}
