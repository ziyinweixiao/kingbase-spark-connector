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

import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;

public class KesTlsContextFactoryTest {

    @Test
    public void loadsEveryCertificateFromPemChain() throws Exception {
        KeyStore trustStore =
                KesTlsContextFactory.createTrustStore(
                        HttpsTestServer.resourcePath("/tls/ca-chain.pem"));

        Assert.assertEquals(2, trustStore.size());
    }

    @Test
    public void disabledTlsDoesNotReadConfiguredMissingCa() throws Exception {
        KesTlsOptions options = options(false, "missing-ca.pem");

        Assert.assertNotNull(KesTlsContextFactory.createSslContext(options));
    }

    @Test
    public void missingCaErrorContainsConfiguredAndAbsolutePaths() throws Exception {
        String configuredPath = "missing-ca.pem";

        try {
            KesTlsContextFactory.createSslContext(options(true, configuredPath));
            Assert.fail("Expected the missing CA file to be rejected");
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage().contains(configuredPath));
            Assert.assertTrue(
                    e.getMessage().contains(Paths.get(configuredPath).toAbsolutePath().toString()));
        }
    }

    private static KesTlsOptions options(boolean enabled, String caPath) throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put(KesOptions.KES_FENODES.getName(), "localhost:8030");
        values.put(KesOptions.KES_TABLE_IDENTIFIER.getName(), "db.tbl");
        values.put(KesOptions.KES_USER.getName(), "root");
        values.put(KesOptions.KES_PASSWORD.getName(), "");
        values.put(KesOptions.KES_ENABLE_TLS.getName(), Boolean.toString(enabled));
        values.put(KesOptions.KES_TLS_CA_CERTIFICATE_PATH.getName(), caPath);
        return KesConfig.fromMap(values, false).getTlsOptions();
    }
}
