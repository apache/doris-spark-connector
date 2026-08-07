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

package org.apache.doris.spark.config;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

public class DorisTlsOptionsTest {

    @Test
    public void defaultsToPlaintextForEveryProtocol() throws Exception {
        DorisTlsOptions options = DorisTlsOptions.fromConfig(config(new HashMap<>()));

        Assert.assertFalse(options.isEnabled());
        Assert.assertEquals("", options.getCaCertificatePath());
        Assert.assertFalse(options.isSkipHostnameVerification());
        for (DorisTlsOptions.Protocol protocol : DorisTlsOptions.Protocol.values()) {
            Assert.assertFalse(options.isEnabledFor(protocol));
        }
    }

    @Test
    public void enablesTlsExceptForExplicitProtocols() throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put("doris.enable.tls", "true");
        values.put("doris.tls.ca-certificate-path", "/certs/ca-chain.pem");
        values.put("doris.tls.skip-hostname-verification", "true");
        values.put("doris.tls.excluded-protocols", " HTTP, thrift ");

        DorisTlsOptions options = DorisTlsOptions.fromConfig(config(values));

        Assert.assertTrue(options.isEnabled());
        Assert.assertEquals("/certs/ca-chain.pem", options.getCaCertificatePath());
        Assert.assertTrue(options.isSkipHostnameVerification());
        Assert.assertFalse(options.isEnabledFor(DorisTlsOptions.Protocol.HTTP));
        Assert.assertTrue(options.isEnabledFor(DorisTlsOptions.Protocol.MYSQL));
        Assert.assertFalse(options.isEnabledFor(DorisTlsOptions.Protocol.THRIFT));
        Assert.assertTrue(options.isEnabledFor(DorisTlsOptions.Protocol.ARROW_FLIGHT));
    }

    @Test
    public void rejectsUnknownExcludedProtocol() throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put("doris.enable.tls", "true");
        values.put("doris.tls.excluded-protocols", "http,grpc");

        try {
            DorisTlsOptions.fromConfig(config(values));
            Assert.fail("Expected an invalid excluded protocol error");
        } catch (java.lang.IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("grpc"));
        }
    }

    @Test
    public void rejectsEmptyExcludedProtocol() throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put("doris.enable.tls", "true");
        values.put("doris.tls.excluded-protocols", "http,");

        try {
            DorisTlsOptions.fromConfig(config(values));
            Assert.fail("Expected an empty excluded protocol error");
        } catch (java.lang.IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("Unknown TLS excluded protocol"));
        }
    }

    @Test
    public void survivesJavaSerialization() throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put("doris.enable.tls", "true");
        values.put("doris.tls.ca-certificate-path", "/certs/ca.pem");
        values.put("doris.tls.excluded-protocols", "mysql");
        DorisTlsOptions original = DorisTlsOptions.fromConfig(config(values));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(original);
        }
        DorisTlsOptions restored;
        try (ObjectInputStream input =
                     new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (DorisTlsOptions) input.readObject();
        }

        Assert.assertEquals(original, restored);
        Assert.assertFalse(restored.isEnabledFor(DorisTlsOptions.Protocol.MYSQL));
    }

    private static DorisConfig config(Map<String, String> tlsValues) throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put(DorisOptions.DORIS_FENODES.getName(), "localhost:8030");
        values.put(DorisOptions.DORIS_TABLE_IDENTIFIER.getName(), "db.tbl");
        values.put(DorisOptions.DORIS_USER.getName(), "root");
        values.put(DorisOptions.DORIS_PASSWORD.getName(), "");
        values.putAll(tlsValues);
        return DorisConfig.fromMap(values, false);
    }
}
