package org.apache.doris.spark.util

import org.apache.doris.spark.config.{DorisConfig, DorisConfigOptions}
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
    val enableHttps = config.getValue(DorisConfigOptions.DORIS_ENABLE_HTTPS)
    if (enableHttps) {
      require(config.contains(DorisConfigOptions.DORIS_HTTPS_KEY_STORE_PATH))
      val keyStorePath: String = config.getValue(DorisConfigOptions.DORIS_HTTPS_KEY_STORE_PATH)
      val keyStoreFile = new File(keyStorePath)
      if (!keyStoreFile.exists()) throw new IllegalArgumentException()
      val keyStoreType: String = config.getValue(DorisConfigOptions.DORIS_HTTPS_KEY_STORE_TYPE)
      val keyStore = KeyStore.getInstance(keyStoreType)
      var fis: FileInputStream = null
      Try {
        fis = new FileInputStream(keyStoreFile)
        val password = config.getValue(DorisConfigOptions.DORIS_HTTPS_KEY_STORE_PASSWORD)
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
