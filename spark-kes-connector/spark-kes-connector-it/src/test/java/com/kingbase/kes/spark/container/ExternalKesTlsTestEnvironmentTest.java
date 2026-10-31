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

package com.kingbase.kes.spark.container;

import org.junit.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ExternalKesTlsTestEnvironmentTest {

    @Test
    public void disabledWhenOptInFlagIsMissing() {
        assertFalse(ExternalKesTlsTestEnvironment.isEnabled(new Properties()));

        Properties properties = new Properties();
        properties.setProperty("kes_tls_test", "false");
        assertFalse(ExternalKesTlsTestEnvironment.isEnabled(properties));

        properties.setProperty("kes_tls_test", "true");
        assertTrue(ExternalKesTlsTestEnvironment.isEnabled(properties));
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
        properties.setProperty("kes_tls_test", "sometimes");

        assertInvalidEnabled(properties, "kes_tls_test");
    }

    @Test
    public void requiresCaCertificatePath() {
        Properties properties = validProperties();
        properties.remove("kes_tls_ca_certificate_path");

        assertInvalid(properties, "kes_tls_ca_certificate_path");
    }

    @Test
    public void rejectsInvalidPorts() {
        Properties properties = validProperties();
        properties.setProperty("kes_query_port", "0");
        assertInvalid(properties, "kes_query_port");

        properties = validProperties();
        properties.setProperty("kes_flight_sql_port", "not-a-port");
        assertInvalid(properties, "kes_flight_sql_port");
    }

    @Test
    public void rejectsInvalidHostnameVerificationBoolean() {
        Properties properties = validProperties();
        properties.setProperty("kes_tls_skip_hostname_verification", "sometimes");

        assertInvalid(properties, "kes_tls_skip_hostname_verification");
    }

    @Test
    public void buildsStrictTlsOptionsForArrowRead() {
        ExternalKesTlsTestEnvironment environment =
                ExternalKesTlsTestEnvironment.fromProperties(validProperties());

        Map<String, String> options = environment.connectorOptions("db.source", "arrow");

        assertEquals("192.0.2.10:8030", options.get("kes.fenodes"));
        assertEquals("db.source", options.get("kes.table.identifier"));
        assertEquals("root", options.get("kes.user"));
        assertEquals("", options.get("kes.password"));
        assertEquals("true", options.get("kes.enable.tls"));
        assertEquals(
                "/certs/ca.pem", options.get("kes.tls.ca-certificate-path"));
        assertEquals(
                "false", options.get("kes.tls.skip-hostname-verification"));
        assertEquals(
                "arrowflight", options.get("kes.tls.excluded-protocols"));
        assertEquals("arrow", options.get("kes.read.mode"));
        assertEquals("8070", options.get("kes.read.arrow-flight-sql.port"));
    }

    @Test
    public void omitsFlightPortForThriftReadAndAcceptsEmptyPassword() {
        ExternalKesTlsTestEnvironment environment =
                ExternalKesTlsTestEnvironment.fromProperties(validProperties());

        Map<String, String> options = environment.connectorOptions("db.source", "thrift");

        assertEquals("", options.get("kes.password"));
        assertEquals("thrift", options.get("kes.read.mode"));
        assertFalse(options.containsKey("kes.read.arrow-flight-sql.port"));
    }

    private static Properties validProperties() {
        Properties properties = new Properties();
        properties.setProperty("kes_tls_test", "true");
        properties.setProperty("customer_env", "true");
        properties.setProperty("kes_host", "192.0.2.10");
        properties.setProperty("kes_query_port", "9030");
        properties.setProperty("kes_http_port", "8030");
        properties.setProperty("kes_user", "root");
        properties.setProperty("kes_passwd", "");
        properties.setProperty("kes_tls_ca_certificate_path", "/certs/ca.pem");
        properties.setProperty("kes_tls_skip_hostname_verification", "false");
        properties.setProperty("kes_tls_excluded_protocols", "arrowflight");
        properties.setProperty("kes_flight_sql_port", "8070");
        return properties;
    }

    private static void assertInvalid(Properties properties, String propertyName) {
        try {
            ExternalKesTlsTestEnvironment.fromProperties(properties);
            fail("Expected invalid property: " + propertyName);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains(propertyName));
        }
    }

    private static void assertInvalidEnabled(Properties properties, String propertyName) {
        try {
            ExternalKesTlsTestEnvironment.isEnabled(properties);
            fail("Expected invalid property: " + propertyName);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains(propertyName));
        }
    }
}
