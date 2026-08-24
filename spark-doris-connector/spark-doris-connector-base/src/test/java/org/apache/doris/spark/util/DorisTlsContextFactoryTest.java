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

import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;

public class DorisTlsContextFactoryTest {

    @Test
    public void loadsEveryCertificateFromPemChain() throws Exception {
        KeyStore trustStore =
                DorisTlsContextFactory.createTrustStore(
                        HttpsTestServer.resourcePath("/tls/ca-chain.pem"));

        Assert.assertEquals(2, trustStore.size());
    }

    @Test
    public void disabledTlsDoesNotReadConfiguredMissingCa() throws Exception {
        DorisTlsOptions options = options(false, "missing-ca.pem");

        Assert.assertNotNull(DorisTlsContextFactory.createSslContext(options));
    }

    @Test
    public void missingCaErrorContainsConfiguredAndAbsolutePaths() throws Exception {
        String configuredPath = "missing-ca.pem";

        try {
            DorisTlsContextFactory.createSslContext(options(true, configuredPath));
            Assert.fail("Expected the missing CA file to be rejected");
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage().contains(configuredPath));
            Assert.assertTrue(
                    e.getMessage().contains(Paths.get(configuredPath).toAbsolutePath().toString()));
        }
    }

    private static DorisTlsOptions options(boolean enabled, String caPath) throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put(DorisOptions.DORIS_FENODES.getName(), "localhost:8030");
        values.put(DorisOptions.DORIS_TABLE_IDENTIFIER.getName(), "db.tbl");
        values.put(DorisOptions.DORIS_USER.getName(), "root");
        values.put(DorisOptions.DORIS_PASSWORD.getName(), "");
        values.put(DorisOptions.DORIS_ENABLE_TLS.getName(), Boolean.toString(enabled));
        values.put(DorisOptions.DORIS_TLS_CA_CERTIFICATE_PATH.getName(), caPath);
        return DorisConfig.fromMap(values, false).getTlsOptions();
    }
}
