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

package org.apache.doris.spark.util;

import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.config.DorisOptions;
import org.apache.doris.spark.config.DorisTlsOptions;
import org.apache.doris.spark.testutil.HttpsTestServer;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DorisJdbcTlsAdapterTest {

    @Test
    public void plaintextConnectionPropertiesContainOnlyCredentials() throws Exception {
        try (DorisJdbcTlsAdapter adapter = DorisJdbcTlsAdapter.create(plaintextOptions())) {
            Properties properties = adapter.createConnectionProperties("root", "secret");

            Assert.assertEquals("root", properties.getProperty("user"));
            Assert.assertEquals("secret", properties.getProperty("password"));
            Assert.assertNull(properties.getProperty("sslMode"));
            Assert.assertNull(properties.getProperty("trustCertificateKeyStoreUrl"));
        }
    }

    @Test
    public void strictTlsCreatesLifecycleScopedPkcs12TrustStore() throws Exception {
        Path trustStore;
        try (DorisJdbcTlsAdapter adapter =
                DorisJdbcTlsAdapter.create(
                        HttpsTestServer.tlsOptions("/tls/ca-chain.pem", false))) {
            Properties properties = adapter.createConnectionProperties("root", "secret");

            Assert.assertEquals("VERIFY_IDENTITY", properties.getProperty("sslMode"));
            Assert.assertEquals("PKCS12", properties.getProperty("trustCertificateKeyStoreType"));
            Assert.assertNotNull(properties.getProperty("trustCertificateKeyStorePassword"));
            trustStore =
                    Paths.get(
                            URI.create(properties.getProperty("trustCertificateKeyStoreUrl")));
            Assert.assertTrue(Files.exists(trustStore));
        }
        Assert.assertFalse(Files.exists(trustStore));
    }

    @Test
    public void hostnameSkipKeepsCaVerification() throws Exception {
        try (DorisJdbcTlsAdapter adapter =
                DorisJdbcTlsAdapter.create(
                        HttpsTestServer.tlsOptions("/tls/ca.pem", true))) {
            Properties properties = adapter.createConnectionProperties("root", "secret");

            Assert.assertEquals("VERIFY_CA", properties.getProperty("sslMode"));
            Assert.assertNotNull(properties.getProperty("trustCertificateKeyStoreUrl"));
        }
    }

    @Test
    public void tlsWithoutCustomCaUsesConnectorDefaultTrust() throws Exception {
        try (DorisJdbcTlsAdapter adapter = DorisJdbcTlsAdapter.create(tlsWithoutCa())) {
            Properties properties = adapter.createConnectionProperties("root", "secret");

            Assert.assertEquals("VERIFY_IDENTITY", properties.getProperty("sslMode"));
            Assert.assertNull(properties.getProperty("trustCertificateKeyStoreUrl"));
        }
    }

    @Test
    public void rejectsJdbcUrlTlsPropertiesManagedByConnector() throws Exception {
        try (DorisJdbcTlsAdapter adapter = DorisJdbcTlsAdapter.create(tlsWithoutCa())) {
            try {
                adapter.validateJdbcUrl("jdbc:mysql://localhost:9030/?sslMode=DISABLED");
                Assert.fail("Expected conflicting JDBC TLS property to be rejected");
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage().contains("sslMode"));
            }
        }
    }

    private static DorisTlsOptions plaintextOptions() throws Exception {
        return config(false, "").getTlsOptions();
    }

    private static DorisTlsOptions tlsWithoutCa() throws Exception {
        return config(true, "").getTlsOptions();
    }

    private static DorisConfig config(boolean enabled, String caPath) throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put(DorisOptions.DORIS_FENODES.getName(), "localhost:8030");
        values.put(DorisOptions.DORIS_TABLE_IDENTIFIER.getName(), "db.tbl");
        values.put(DorisOptions.DORIS_USER.getName(), "root");
        values.put(DorisOptions.DORIS_PASSWORD.getName(), "");
        values.put(DorisOptions.DORIS_ENABLE_TLS.getName(), Boolean.toString(enabled));
        values.put(DorisOptions.DORIS_TLS_CA_CERTIFICATE_PATH.getName(), caPath);
        return DorisConfig.fromMap(values, false);
    }
}
