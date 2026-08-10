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
import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.testutil.HttpsTestServer;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.junit.Assert;
import org.junit.Test;

import javax.net.ssl.SSLHandshakeException;

import java.util.Collections;

public class DorisBackendHttpClientTest {

    @Test
    public void requestsBackendThroughConfiguredTls() throws Exception {
        try (HttpsTestServer server = new HttpsTestServer()) {
            DorisConfig config = HttpsTestServer.tlsConfig("/tls/ca.pem", false);
            DorisBackendHttpClient client =
                    new DorisBackendHttpClient(
                            Collections.singletonList(
                                    new Backend("localhost", server.getPort(), -1)),
                            config);
            try {
                int status = client.executeReq((backend, httpClient) -> {
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
    public void preservesBackendTlsProbeFailure() throws Exception {
        try (HttpsTestServer server = new HttpsTestServer()) {
            DorisConfig config = HttpsTestServer.tlsConfig("/tls/wrong-ca.pem", false);
            DorisBackendHttpClient client =
                    new DorisBackendHttpClient(
                            Collections.singletonList(
                                    new Backend("localhost", server.getPort(), -1)),
                            config);
            try {
                client.executeReq((backend, httpClient) -> {
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

    private static boolean hasCause(Throwable failure, Class<? extends Throwable> causeType) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (causeType.isInstance(current)) {
                return true;
            }
        }
        return false;
    }
}
