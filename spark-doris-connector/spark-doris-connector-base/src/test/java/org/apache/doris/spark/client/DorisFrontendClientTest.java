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

package org.apache.doris.spark.client;

import org.apache.doris.spark.client.entity.Backend;
import org.apache.doris.spark.client.entity.Frontend;
import org.apache.doris.spark.testutil.HttpsTestServer;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import javax.net.ssl.SSLHandshakeException;

import java.util.Arrays;
import java.util.List;

public class DorisFrontendClientTest {

    @Test
    public void requestsFrontendThroughConfiguredTls() throws Exception {
        try (HttpsTestServer server = new HttpsTestServer()) {
            DorisFrontendClient client =
                    new DorisFrontendClient(
                            HttpsTestServer.tlsConfig(
                                    server.getEndpoint("localhost"),
                                    "/tls/ca.pem",
                                    false));
            try {
                int status = client.requestFrontends((frontend, httpClient) -> {
                    try (CloseableHttpResponse response =
                            httpClient.execute(new HttpGet(server.getUrl("localhost")))) {
                        return response.getStatusLine().getStatusCode();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

                Assert.assertEquals(200, status);
            } finally {
                client.close();
            }
        }
    }

    @Test
    public void preservesFrontendTlsProbeFailure() throws Exception {
        try (HttpsTestServer server = new HttpsTestServer()) {
            DorisFrontendClient client =
                    new DorisFrontendClient(
                            HttpsTestServer.tlsConfig(
                                    server.getEndpoint("localhost"),
                                    "/tls/wrong-ca.pem",
                                    false));
            try {
                client.requestFrontends((frontend, httpClient) -> {
                    Assert.fail("The request must not run after the TLS probe fails");
                    return null;
                });
                Assert.fail("Expected the TLS probe to fail");
            } catch (Exception e) {
                Assert.assertTrue(hasCause(e, SSLHandshakeException.class));
            } finally {
                client.close();
            }
        }
    }

    @Test
    public void parseManagerBackendsFilterComputeGroup() {
        String response = "{\"columnNames\":[\"BackendId\",\"Host\",\"HttpPort\",\"Alive\",\"Tag\"],"
                + "\"rows\":["
                + "[\"1\",\"192.168.1.1\",\"8042\",\"true\",\"{\\\"compute_group_name\\\":\\\"cluster_a\\\"}\"],"
                + "[\"2\",\"192.168.1.2\",\"8042\",\"true\",\"{\\\"compute_group_name\\\":\\\"cluster_b\\\"}\"],"
                + "[\"3\",\"192.168.1.3\",\"8042\",\"false\",\"{\\\"compute_group_name\\\":\\\"cluster_a\\\"}\"]"
                + "]}";

        List<Backend> backends = DorisFrontendClient.parseManagerBackends(response, "cluster_a");

        Assert.assertEquals(1, backends.size());
        Assert.assertEquals(new Backend("192.168.1.1", 8042, -1), backends.get(0));
    }

    @Test
    public void parseManagerBackendsColumnNamesAndCloudCluster() {
        String response = "{\"code\":0,\"msg\":\"success\",\"data\":{"
                + "\"column_names\":[\"BackendId\",\"Host\",\"HttpPort\",\"Alive\",\"Tag\"],"
                + "\"rows\":["
                + "[\"1\",\"192.168.1.1\",\"8042\",\"true\",\"{\\\"cloud_cluster_name\\\":\\\"cluster_a\\\"}\"]"
                + "]}}";

        List<Backend> backends = DorisFrontendClient.parseManagerBackends(response, "cluster_a");

        Assert.assertEquals(1, backends.size());
        Assert.assertEquals(new Backend("192.168.1.1", 8042, -1), backends.get(0));
    }

    @Test
    public void parseManagerBackendsPrintableTag() {
        Assert.assertEquals("cluster_a",
                DorisFrontendClient.getComputeGroupNameFromTag("{compute_group_name:cluster_a, location:default}"));
    }

    @Test
    public void parseManagerBackendsNoTargetBackend() {
        String response = "{\"columnNames\":[\"BackendId\",\"Host\",\"HttpPort\",\"Alive\",\"Tag\"],"
                + "\"rows\":["
                + "[\"1\",\"192.168.1.1\",\"8042\",\"true\",\"{\\\"compute_group_name\\\":\\\"cluster_a\\\"}\"],"
                + "[\"2\",\"192.168.1.2\",\"8042\",\"false\",\"{\\\"compute_group_name\\\":\\\"cluster_b\\\"}\"]"
                + "]}";

        try {
            DorisFrontendClient.parseManagerBackends(response, "cluster_b");
            Assert.fail("Expected no alive backend exception");
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("no alive backend found"));
        }
    }

    @Test
    public void findArrowFlightSqlPortReturnsFirstFrontendPort() {
        List<Frontend> frontends = Arrays.asList(
                new Frontend("192.168.1.1", 8030, 9030, -1),
                new Frontend("192.168.1.2", 8030, 9030, 9040));

        Assert.assertEquals(-1, DorisFrontendClient.findArrowFlightSqlPort(frontends));
    }

    @Test
    public void findArrowFlightSqlPortReturnsInvalidPortWhenMissing() {
        List<Frontend> frontends = Arrays.asList(
                new Frontend("192.168.1.1", 8030, 9030, -1),
                new Frontend("192.168.1.2", 8030, 9030, 0));

        Assert.assertEquals(-1, DorisFrontendClient.findArrowFlightSqlPort(frontends));
    }

    @Test
    public void parseFrontendsIncludesArrowFlightSqlPort() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode columnNames = (ArrayNode) mapper.readTree(
                "[\"Host\",\"HttpPort\",\"QueryPort\",\"ArrowFlightSqlPort\"]");
        ArrayNode rows = (ArrayNode) mapper.readTree(
                "[[\"192.168.1.1\",\"8030\",\"9030\",\"9040\"]]");

        List<Frontend> frontends = new DorisFrontendClient().parseFrontends(columnNames, rows);

        Assert.assertEquals(9040, DorisFrontendClient.findArrowFlightSqlPort(frontends));
    }

    private static boolean hasCause(Throwable failure, Class<? extends Throwable> causeType) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (causeType.isInstance(current)) {
                return true;
            }
        }
        return false;
    }
}
