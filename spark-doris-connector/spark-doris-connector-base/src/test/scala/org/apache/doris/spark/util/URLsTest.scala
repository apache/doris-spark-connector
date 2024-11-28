package org.apache.doris.spark.util

import org.apache.doris.spark.util.URLs.schema
import org.junit.Assert
import org.junit.jupiter.api.Test

class URLsTest {

  @Test
  def aliveBackendTest(): Unit = {
    Assert.assertEquals(URLs.aliveBackend("127.0.0.1:8030"), "http://127.0.0.1:8030/api/backends?is_alive=true")
    Assert.assertEquals(URLs.aliveBackend("127.0.0.1:8030", enableHttps = true), "https://127.0.0.1:8030/api/backends?is_alive=true")
  }

  @Test
  def tableSchemaTest(): Unit = {
    Assert.assertEquals(URLs.tableSchema("127.0.0.1", 8030, "db", "tbl", enableHttps = false), "http://127.0.0.1:8030/api/db/tbl/_schema")
    Assert.assertEquals(URLs.tableSchema("127.0.0.1", 8030, "db", "tbl", enableHttps = true), "https://127.0.0.1:8030/api/db/tbl/_schema")
  }

  @Test
  def queryPlanTest(): Unit = {
    Assert.assertEquals(URLs.queryPlan("127.0.0.1", 8030, "db", "tbl", enableHttps = false), "http://127.0.0.1:8030/api/db/tbl/_query_plan")
    Assert.assertEquals(URLs.queryPlan("127.0.0.1", 8030, "db", "tbl", enableHttps = true), "https://127.0.0.1:8030/api/db/tbl/_query_plan")
  }

  @Test
  def streamLoadTest(): Unit = {
    Assert.assertEquals(URLs.streamLoad("127.0.0.1", 8030, "db", "tbl", enableHttps = false), "http://127.0.0.1:8030/api/db/tbl/_stream_load")
    Assert.assertEquals(URLs.streamLoad("127.0.0.1", 8030, "db", "tbl", enableHttps = true), "https://127.0.0.1:8030/api/db/tbl/_stream_load")
  }

  @Test
  def streamLoad2PCTest(): Unit = {
    Assert.assertEquals(URLs.streamLoad2PC("127.0.0.1", 8030, "db", enableHttps = false), "http://127.0.0.1:8030/api/db/_stream_load_2pc")
    Assert.assertEquals(URLs.streamLoad2PC("127.0.0.1", 8030, "db", enableHttps = true), "https://127.0.0.1:8030/api/db/_stream_load_2pc")
  }

  @Test
  def getFrontEndNodesTest(): Unit = {
    Assert.assertEquals(URLs.getFrontEndNodes("127.0.0.1", 8030), "http://127.0.0.1:8030/rest/v2/manager/node/frontends")
    Assert.assertEquals(URLs.getFrontEndNodes("127.0.0.1", 8030, enableHttps = true), "https://127.0.0.1:8030/rest/v2/manager/node/frontends")
  }

}
