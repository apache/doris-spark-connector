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
import org.apache.http.conn.ssl.{SSLConnectionSocketFactory, TrustAllStrategy}
import org.apache.http.impl.client.{CloseableHttpClient, DefaultRedirectStrategy, HttpClients}
import org.apache.http.ssl.SSLContexts

import java.io.{File, FileInputStream}
import java.nio.charset.StandardCharsets
import java.security.KeyStore
import java.util.Base64
import scala.util.{Failure, Success, Try}

object HttpUtils {

  def getHttpClient(config: DorisConfig): CloseableHttpClient = {
    val builder = HttpClients.custom().setRedirectStrategy(new DefaultRedirectStrategy {
      override def isRedirectable(method: String): Boolean = true
    })
    val enableHttps = config.getValue(DorisOptions.DORIS_ENABLE_HTTPS)
    if (enableHttps) {
      require(config.contains(DorisOptions.DORIS_HTTPS_KEY_STORE_PATH))
      val keyStorePath: String = config.getValue(DorisOptions.DORIS_HTTPS_KEY_STORE_PATH)
      val keyStoreFile = new File(keyStorePath)
      if (!keyStoreFile.exists()) throw new IllegalArgumentException()
      val keyStoreType: String = config.getValue(DorisOptions.DORIS_HTTPS_KEY_STORE_TYPE)
      val keyStore = KeyStore.getInstance(keyStoreType)
      var fis: FileInputStream = null
      Try {
        fis = new FileInputStream(keyStoreFile)
        val password = config.getValue(DorisOptions.DORIS_HTTPS_KEY_STORE_PASSWORD)
        keyStore.load(fis, if (password == null) null else password.toCharArray)
      } match {
        case Success(_) => if (fis != null) fis.close()
        case Failure(e) =>
          if (fis != null) fis.close()
          throw e
      }
      val sslContext = SSLContexts.custom().loadTrustMaterial(keyStore, new TrustAllStrategy).build()
      builder.setSSLSocketFactory(new SSLConnectionSocketFactory(sslContext))
    }
    builder.build()
  }

  def setAuth(req: HttpRequestBase, user: String, passwd: String): Unit = {
    val authInfo = s"Basic ${Base64.getEncoder.encodeToString((user + ":" + passwd).getBytes(StandardCharsets.UTF_8))}"
    req.setHeader(HttpHeaders.AUTHORIZATION, authInfo)
  }

}
