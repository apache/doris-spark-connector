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
