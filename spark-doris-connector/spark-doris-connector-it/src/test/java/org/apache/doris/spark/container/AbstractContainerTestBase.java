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

package org.apache.doris.spark.container;

import org.apache.doris.spark.container.instance.ContainerService;
import org.apache.doris.spark.container.instance.DorisContainer;
import org.apache.doris.spark.container.instance.DorisCustomerContainer;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public abstract class AbstractContainerTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractContainerTestBase.class);
    protected static ContainerService dorisContainerService;
    public static final int DEFAULT_PARALLELISM = 2;

    @BeforeClass
    public static void initContainers() {
        LOG.info("Trying to start doris containers.");
        initDorisContainer();
    }

    private static void initDorisContainer() {
        if (Objects.nonNull(dorisContainerService) && dorisContainerService.isRunning()) {
            LOG.info("The doris container has been started and is running status.");
            return;
        }
        Boolean customerEnv = Boolean.valueOf(System.getProperty("customer_env", "false"));
        dorisContainerService = customerEnv ? new DorisCustomerContainer() : new DorisContainer();
        dorisContainerService.startContainer();
        LOG.info("Doris container was started.");
    }

    protected static Connection getDorisQueryConnection() {
        return dorisContainerService.getQueryConnection();
    }

    protected static Connection getDorisQueryConnection(String database) {
        return dorisContainerService.getQueryConnection(database);
    }

    protected String getFenodes() {
        return dorisContainerService.getFenodes();
    }

    protected String getBenodes() {
        return dorisContainerService.getBenodes();
    }

    protected String getDorisUsername() {
        return dorisContainerService.getUsername();
    }

    protected String getDorisPassword() {
        return dorisContainerService.getPassword();
    }

    protected int getQueryPort() {
        return dorisContainerService.getQueryPort();
    }

    protected String getDorisQueryUrl() {
        return dorisContainerService.getJdbcUrl();
    }

    protected String getDorisInstanceHost() {
        return dorisContainerService.getInstanceHost();
    }

    public static void closeContainers() {
        LOG.info("Starting to close containers.");
        closeDorisContainer();
    }

    private static void closeDorisContainer() {
        if (Objects.isNull(dorisContainerService)) {
            return;
        }
        dorisContainerService.close();
        LOG.info("Doris container was closed.");
    }

    // ------------------------------------------------------------------------
    //  test utilities
    // ------------------------------------------------------------------------
    public static void assertEqualsInAnyOrder(List<Object> expected, List<Object> actual) {
        assertTrue(expected != null && actual != null);
        assertEqualsInOrder(
                expected.stream().sorted().collect(Collectors.toList()),
                actual.stream().sorted().collect(Collectors.toList()));
    }

    public static void assertEqualsInOrder(List<Object> expected, List<Object> actual) {
        assertTrue(expected != null && actual != null);
        assertEquals(expected.size(), actual.size());
        assertArrayEquals(expected.toArray(new Object[0]), actual.toArray(new Object[0]));
    }

    protected void faultInjectionOpen() throws IOException {
        String pointName = "FlushToken.submit_flush_error";
        String apiUrl =
                String.format(
                        "http://%s/api/debug_point/add/%s",
                        dorisContainerService.getBenodes(), pointName);
        HttpPost httpPost = new HttpPost(apiUrl);
        httpPost.addHeader(
                HttpHeaders.AUTHORIZATION,
                auth(dorisContainerService.getUsername(), dorisContainerService.getPassword()));
        try (CloseableHttpClient httpClient = HttpClients.custom().build()) {
            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                int statusCode = response.getStatusLine().getStatusCode();
                String reason = response.getStatusLine().toString();
                if (statusCode == 200 && response.getEntity() != null) {
                    LOG.info("Debug point response {}", EntityUtils.toString(response.getEntity()));
                } else {
                    LOG.info("Debug point failed, statusCode: {}, reason: {}", statusCode, reason);
                }
            }
        }
    }

    protected void faultInjectionClear() throws IOException {
        String apiUrl =
                String.format(
                        "http://%s/api/debug_point/clear", dorisContainerService.getBenodes());
        HttpPost httpPost = new HttpPost(apiUrl);
        httpPost.addHeader(
                HttpHeaders.AUTHORIZATION,
                auth(dorisContainerService.getUsername(), dorisContainerService.getPassword()));
        try (CloseableHttpClient httpClient = HttpClients.custom().build()) {
            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                int statusCode = response.getStatusLine().getStatusCode();
                String reason = response.getStatusLine().toString();
                if (statusCode == 200 && response.getEntity() != null) {
                    LOG.info("Debug point response {}", EntityUtils.toString(response.getEntity()));
                } else {
                    LOG.info("Debug point failed, statusCode: {}, reason: {}", statusCode, reason);
                }
            }
        }
    }

    protected String auth(String user, String password) {
        final String authInfo = user + ":" + password;
        byte[] encoded = Base64.encodeBase64(authInfo.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }
}
