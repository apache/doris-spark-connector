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

package org.apache.doris.spark.testutil;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.config.DorisOptions;
import org.apache.doris.spark.config.DorisTlsOptions;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/** Local HTTPS server backed by a test-only localhost certificate. */
public final class HttpsTestServer implements AutoCloseable {

    private static final char[] KEYSTORE_PASSWORD = "changeit".toCharArray();

    private final HttpsServer server;

    public HttpsTestServer() throws Exception {
        this("ok");
    }

    public HttpsTestServer(String responseBody) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream encoded =
                        HttpsTestServer.class.getResourceAsStream(
                                "/tls/server-localhost.p12.b64");
                InputStream decoded =
                        encoded == null ? null : Base64.getMimeDecoder().wrap(encoded)) {
            if (decoded == null) {
                throw new IllegalStateException("Missing TLS server test resource");
            }
            keyStore.load(decoded, KEYSTORE_PASSWORD);
        }

        KeyManagerFactory keyManagerFactory =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, KEYSTORE_PASSWORD);
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), null, null);

        server =
                HttpsServer.create(
                        new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(sslContext));
        server.createContext("/", exchange -> respond(exchange, responseBody));
        server.start();
    }

    public String getUrl(String host) {
        return "https://" + host + ":" + server.getAddress().getPort() + "/";
    }

    public String getEndpoint(String host) {
        return host + ":" + server.getAddress().getPort();
    }

    public int getPort() {
        return server.getAddress().getPort();
    }

    public void redirectTo(String location) {
        server.removeContext("/");
        server.createContext(
                "/",
                exchange -> {
                    exchange.getResponseHeaders().add("Location", location);
                    exchange.sendResponseHeaders(307, -1);
                    exchange.close();
                });
    }

    public static String resourcePath(String resource) throws URISyntaxException {
        Path path = Paths.get(HttpsTestServer.class.getResource(resource).toURI());
        return path.toString();
    }

    public static DorisTlsOptions tlsOptions(String caResource, boolean skipHostname)
            throws Exception {
        return tlsConfig(caResource, skipHostname).getTlsOptions();
    }

    public static DorisConfig tlsConfig(String caResource, boolean skipHostname)
            throws Exception {
        return tlsConfig("localhost:8030", caResource, skipHostname);
    }

    public static DorisConfig tlsConfig(
            String feNodes, String caResource, boolean skipHostname) throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put(DorisOptions.DORIS_FENODES.getName(), feNodes);
        values.put(DorisOptions.DORIS_TABLE_IDENTIFIER.getName(), "db.tbl");
        values.put(DorisOptions.DORIS_USER.getName(), "root");
        values.put(DorisOptions.DORIS_PASSWORD.getName(), "");
        values.put(DorisOptions.DORIS_ENABLE_TLS.getName(), "true");
        values.put(
                DorisOptions.DORIS_TLS_CA_CERTIFICATE_PATH.getName(),
                resourcePath(caResource));
        values.put(
                DorisOptions.DORIS_TLS_SKIP_HOSTNAME_VERIFICATION.getName(),
                Boolean.toString(skipHostname));
        return DorisConfig.fromMap(values, false);
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private static void respond(HttpExchange exchange, String responseBody) throws IOException {
        byte[] buffer = new byte[1024];
        while (exchange.getRequestBody().read(buffer) >= 0) {
            // Drain streaming request bodies before sending the response.
        }
        exchange.getRequestBody().close();
        byte[] response = responseBody.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, response.length);
        exchange.getResponseBody().write(response);
        exchange.close();
    }
}
