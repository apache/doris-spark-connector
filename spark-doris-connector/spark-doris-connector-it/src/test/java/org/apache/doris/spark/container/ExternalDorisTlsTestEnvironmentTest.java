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

import org.junit.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ExternalDorisTlsTestEnvironmentTest {

    @Test
    public void disabledWhenOptInFlagIsMissing() {
        assertFalse(ExternalDorisTlsTestEnvironment.isEnabled(new Properties()));

        Properties properties = new Properties();
        properties.setProperty("doris_tls_test", "false");
        assertFalse(ExternalDorisTlsTestEnvironment.isEnabled(properties));

        properties.setProperty("doris_tls_test", "true");
        assertTrue(ExternalDorisTlsTestEnvironment.isEnabled(properties));
    }

    @Test
    public void requiresCustomerEnvironmentWhenEnabled() {
        Properties properties = validProperties();
        properties.setProperty("customer_env", "false");

        assertInvalid(properties, "customer_env");
    }

    @Test
    public void rejectsInvalidOptInBoolean() {
        Properties properties = new Properties();
        properties.setProperty("doris_tls_test", "sometimes");

        assertInvalidEnabled(properties, "doris_tls_test");
    }

    @Test
    public void requiresCaCertificatePath() {
        Properties properties = validProperties();
        properties.remove("doris_tls_ca_certificate_path");

        assertInvalid(properties, "doris_tls_ca_certificate_path");
    }

    @Test
    public void rejectsInvalidPorts() {
        Properties properties = validProperties();
        properties.setProperty("doris_query_port", "0");
        assertInvalid(properties, "doris_query_port");

        properties = validProperties();
        properties.setProperty("doris_flight_sql_port", "not-a-port");
        assertInvalid(properties, "doris_flight_sql_port");
    }

    @Test
    public void rejectsInvalidHostnameVerificationBoolean() {
        Properties properties = validProperties();
        properties.setProperty("doris_tls_skip_hostname_verification", "sometimes");

        assertInvalid(properties, "doris_tls_skip_hostname_verification");
    }

    @Test
    public void buildsStrictTlsOptionsForArrowRead() {
        ExternalDorisTlsTestEnvironment environment =
                ExternalDorisTlsTestEnvironment.fromProperties(validProperties());

        Map<String, String> options = environment.connectorOptions("db.source", "arrow");

        assertEquals("192.0.2.10:8030", options.get("doris.fenodes"));
        assertEquals("db.source", options.get("doris.table.identifier"));
        assertEquals("root", options.get("doris.user"));
        assertEquals("", options.get("doris.password"));
        assertEquals("true", options.get("doris.enable.tls"));
        assertEquals(
                "/certs/ca.pem", options.get("doris.tls.ca-certificate-path"));
        assertEquals(
                "false", options.get("doris.tls.skip-hostname-verification"));
        assertEquals(
                "arrowflight", options.get("doris.tls.excluded-protocols"));
        assertEquals("arrow", options.get("doris.read.mode"));
        assertEquals("8070", options.get("doris.read.arrow-flight-sql.port"));
    }

    @Test
    public void omitsFlightPortForThriftReadAndAcceptsEmptyPassword() {
        ExternalDorisTlsTestEnvironment environment =
                ExternalDorisTlsTestEnvironment.fromProperties(validProperties());

        Map<String, String> options = environment.connectorOptions("db.source", "thrift");

        assertEquals("", options.get("doris.password"));
        assertEquals("thrift", options.get("doris.read.mode"));
        assertFalse(options.containsKey("doris.read.arrow-flight-sql.port"));
    }

    private static Properties validProperties() {
        Properties properties = new Properties();
        properties.setProperty("doris_tls_test", "true");
        properties.setProperty("customer_env", "true");
        properties.setProperty("doris_host", "192.0.2.10");
        properties.setProperty("doris_query_port", "9030");
        properties.setProperty("doris_http_port", "8030");
        properties.setProperty("doris_user", "root");
        properties.setProperty("doris_passwd", "");
        properties.setProperty("doris_tls_ca_certificate_path", "/certs/ca.pem");
        properties.setProperty("doris_tls_skip_hostname_verification", "false");
        properties.setProperty("doris_tls_excluded_protocols", "arrowflight");
        properties.setProperty("doris_flight_sql_port", "8070");
        return properties;
    }

    private static void assertInvalid(Properties properties, String propertyName) {
        try {
            ExternalDorisTlsTestEnvironment.fromProperties(properties);
            fail("Expected invalid property: " + propertyName);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains(propertyName));
        }
    }

    private static void assertInvalidEnabled(Properties properties, String propertyName) {
        try {
            ExternalDorisTlsTestEnvironment.isEnabled(properties);
            fail("Expected invalid property: " + propertyName);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains(propertyName));
        }
    }
}
