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

package com.kingbase.kes.spark.util;

import com.kingbase.kes.spark.config.KesConfig;
import com.kingbase.kes.spark.config.KesOptions;
import com.kingbase.kes.spark.config.KesTlsOptions;
import com.kingbase.kes.spark.testutil.HttpsTestServer;
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

public class KesJdbcTlsAdapterTest {

    @Test
    public void plaintextConnectionPropertiesContainOnlyCredentials() throws Exception {
        try (KesJdbcTlsAdapter adapter = KesJdbcTlsAdapter.create(plaintextOptions())) {
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
        try (KesJdbcTlsAdapter adapter =
                KesJdbcTlsAdapter.create(
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
        try (KesJdbcTlsAdapter adapter =
                KesJdbcTlsAdapter.create(
                        HttpsTestServer.tlsOptions("/tls/ca.pem", true))) {
            Properties properties = adapter.createConnectionProperties("root", "secret");

            Assert.assertEquals("VERIFY_CA", properties.getProperty("sslMode"));
            Assert.assertNotNull(properties.getProperty("trustCertificateKeyStoreUrl"));
        }
    }

    @Test
    public void tlsWithoutCustomCaUsesConnectorDefaultTrust() throws Exception {
        try (KesJdbcTlsAdapter adapter = KesJdbcTlsAdapter.create(tlsWithoutCa())) {
            Properties properties = adapter.createConnectionProperties("root", "secret");

            Assert.assertEquals("VERIFY_IDENTITY", properties.getProperty("sslMode"));
            Assert.assertNull(properties.getProperty("trustCertificateKeyStoreUrl"));
        }
    }

    @Test
    public void rejectsJdbcUrlTlsPropertiesManagedByConnector() throws Exception {
        try (KesJdbcTlsAdapter adapter = KesJdbcTlsAdapter.create(tlsWithoutCa())) {
            try {
                adapter.validateJdbcUrl("jdbc:mysql://localhost:9030/?sslMode=DISABLED");
                Assert.fail("Expected conflicting JDBC TLS property to be rejected");
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage().contains("sslMode"));
            }
        }
    }

    private static KesTlsOptions plaintextOptions() throws Exception {
        return config(false, "").getTlsOptions();
    }

    private static KesTlsOptions tlsWithoutCa() throws Exception {
        return config(true, "").getTlsOptions();
    }

    private static KesConfig config(boolean enabled, String caPath) throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put(KesOptions.KES_FENODES.getName(), "localhost:8030");
        values.put(KesOptions.KES_TABLE_IDENTIFIER.getName(), "db.tbl");
        values.put(KesOptions.KES_USER.getName(), "root");
        values.put(KesOptions.KES_PASSWORD.getName(), "");
        values.put(KesOptions.KES_ENABLE_TLS.getName(), Boolean.toString(enabled));
        values.put(KesOptions.KES_TLS_CA_CERTIFICATE_PATH.getName(), caPath);
        return KesConfig.fromMap(values, false);
    }
}
