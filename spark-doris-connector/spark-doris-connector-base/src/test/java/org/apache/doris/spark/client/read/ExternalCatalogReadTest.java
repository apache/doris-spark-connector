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

package org.apache.doris.spark.client.read;

import org.apache.doris.spark.client.entity.DorisReaderPartition;
import org.apache.doris.spark.config.DorisConfig;
import org.junit.Assert;
import org.junit.Test;

import com.sun.net.httpserver.HttpServer;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class ExternalCatalogReadTest {

    @Test
    public void catalogScanUsesCatalogSchemaAndOneTabletFreePartition() throws Exception {
        List<String> requests = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            requests.add(path);
            String data = path.endsWith("/_schema")
                    ? "{\"properties\":[{\"name\":\"id\",\"type\":\"INT\"},"
                        + "{\"name\":\"name\",\"type\":\"STRING\"}]}"
                    : "{\"status\":0,\"partitions\":{},\"opaqued_query_plan\":\"\"}";
            byte[] response = ("{\"code\":0,\"data\":" + data + "}")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            try (java.io.OutputStream out = exchange.getResponseBody()) {
                out.write(response);
            }
        });
        server.start();
        try {
            DorisConfig config = createConfig("hive.db.tbl", server.getAddress().getPort());
            String[] filters = {"id > 1"};
            DorisReaderPartition[] partitions = ReaderPartitionGenerator.generatePartitions(
                    config, new String[0], filters, 5, false);

            Assert.assertEquals(1, partitions.length);
            Assert.assertEquals("db", partitions[0].getDatabase());
            Assert.assertEquals("tbl", partitions[0].getTable());
            Assert.assertNull(partitions[0].getBackend());
            Assert.assertEquals(0, partitions[0].getTablets().length);
            Assert.assertArrayEquals(new String[]{"`id`", "`name`"}, partitions[0].getReadColumns());
            Assert.assertArrayEquals(filters, partitions[0].getFilters());
            Assert.assertEquals(Integer.valueOf(5), partitions[0].getLimit());
            Assert.assertTrue(requests.contains("/api/hive/db/tbl/_schema"));
            Assert.assertFalse(requests.stream().anyMatch(path -> path.endsWith("/_query_plan")));
            Assert.assertEquals(1L, requests.stream().filter(path -> path.endsWith("/_schema")).count());

            DorisReaderPartition[] projected = ReaderPartitionGenerator.generatePartitions(
                    config, new String[]{"name"}, new String[0], -1, false);
            Assert.assertArrayEquals(new String[]{"`name`"}, projected[0].getReadColumns());
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void identifiersPreserveCatalogAndNormalizeBackticks() throws Exception {
        Assert.assertArrayEquals(new String[]{"hive", "db", "tbl"},
                createConfig("`hive`.`db`.`tbl`", 8030).getTableIdentifierParts());
        Assert.assertArrayEquals(new String[]{"db", "tbl"},
                createConfig("db.tbl", 8030).getTableIdentifierParts());
    }

    @Test
    public void malformedIdentifiersAreRejected() {
        for (String identifier : new String[]{"db", ".tbl", "db.", "cat..tbl", "a.b.c.d", "``.tbl"}) {
            org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                    () -> createConfig(identifier, 8030).getTableIdentifierParts());
        }
    }

    private DorisConfig createConfig(String tableIdentifier, int httpPort) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("doris.fenodes", "127.0.0.1:" + httpPort);
        options.put("doris.fe.auto.fetch", "false");
        options.put("doris.table.identifier", tableIdentifier);
        options.put("doris.user", "root");
        options.put("doris.password", "");
        options.put("doris.read.arrow-flight-sql.port", "9040");
        return DorisConfig.fromMap(options, false);
    }

}
