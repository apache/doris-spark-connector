package org.apache.doris.spark.util

import org.apache.doris.spark.cfg.{ConfigurationOptions, SparkSettings}
import org.apache.http.client.config.RequestConfig
import org.apache.http.conn.ssl.{SSLConnectionSocketFactory, TrustAllStrategy}
import org.apache.http.impl.client.{CloseableHttpClient, DefaultRedirectStrategy, HttpClients}
import org.apache.http.ssl.SSLContexts

import java.io.{File, FileInputStream}
import java.security.KeyStore
import scala.util.{Failure, Success, Try}

object HttpUtil {

  def getHttpClient(settings: SparkSettings): CloseableHttpClient = {
    val connectTimeout = settings.getIntegerProperty(ConfigurationOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS,
      ConfigurationOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT)
    val socketTimeout = settings.getIntegerProperty(ConfigurationOptions.DORIS_REQUEST_READ_TIMEOUT_MS,
      ConfigurationOptions.DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT)
    val requestConfig = RequestConfig.custom().setConnectTimeout(connectTimeout).setSocketTimeout(socketTimeout).build()
    val clientBuilder = HttpClients.custom()
      .setRedirectStrategy(new DefaultRedirectStrategy {
        override def isRedirectable(method: String): Boolean = true
      })
      .setDefaultRequestConfig(requestConfig)
    val enableHttps = settings.getBooleanProperty("doris.enable.https", false)
    if (enableHttps) {
      val props = settings.asProperties()
      require(props.containsKey(ConfigurationOptions.DORIS_HTTPS_KEY_STORE_PATH))
      val keyStorePath: String = props.getProperty(ConfigurationOptions.DORIS_HTTPS_KEY_STORE_PATH)
      val keyStoreFile = new File(keyStorePath)
      if (!keyStoreFile.exists()) throw new IllegalArgumentException()
      val keyStoreType: String = props.getProperty(ConfigurationOptions.DORIS_HTTPS_KEY_STORE_TYPE,
        ConfigurationOptions.DORIS_HTTPS_KEY_STORE_TYPE_DEFAULT)
      val keyStore = KeyStore.getInstance(keyStoreType)
      var fis: FileInputStream = null
      Try {
        fis = new FileInputStream(keyStoreFile)
        val password = props.getProperty(ConfigurationOptions.DORIS_HTTPS_KEY_STORE_PASSWORD)
        keyStore.load(fis, if (password == null) null else password.toCharArray)
      } match {
        case Success(_) => if (fis != null) fis.close()
        case Failure(e) =>
          if (fis != null) fis.close()
          throw e
      }
      val sslContext = SSLContexts.custom().loadTrustMaterial(keyStore, new TrustAllStrategy).build()
      clientBuilder.setSSLSocketFactory(new SSLConnectionSocketFactory(sslContext))
    }
    clientBuilder.build()
  }

}
