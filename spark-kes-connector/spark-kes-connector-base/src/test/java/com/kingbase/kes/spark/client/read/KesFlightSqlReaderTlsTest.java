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

package com.kingbase.kes.spark.client.read;

import com.kingbase.kes.spark.config.KesConfig;
import com.kingbase.kes.spark.config.KesOptions;
import com.kingbase.kes.spark.exception.KesRuntimeException;

import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlConnectionProperties;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class KesFlightSqlReaderTlsTest {

    @Test
    public void plaintextUsesInsecureGrpcUri() throws Exception {
        Map<String, Object> parameters = KesFlightSqlReader.createConnectionParameters(
                "localhost", 8815, config(false, false, ""), null);

        Assert.assertEquals("grpc+tcp://localhost:8815", AdbcDriver.PARAM_URI.get(parameters));
        Assert.assertEquals("root", AdbcDriver.PARAM_USERNAME.get(parameters));
        Assert.assertEquals("secret", AdbcDriver.PARAM_PASSWORD.get(parameters));
        Assert.assertNull(FlightSqlConnectionProperties.TLS_ROOT_CERTS.get(parameters));
    }

    @Test
    public void tlsUsesSecureGrpcUriAndConfiguredRootCertificates() throws Exception {
        InputStream rootCertificates = new ByteArrayInputStream(new byte[] {1, 2, 3});

        Map<String, Object> parameters = KesFlightSqlReader.createConnectionParameters(
                "localhost", 8815, config(true, false, ""), rootCertificates);

        Assert.assertEquals("grpc+tls://localhost:8815", AdbcDriver.PARAM_URI.get(parameters));
        Assert.assertSame(
                rootCertificates,
                FlightSqlConnectionProperties.TLS_ROOT_CERTS.get(parameters));
    }

    @Test
    public void arrowFlightExclusionKeepsPlaintextUri() throws Exception {
        Map<String, Object> parameters = KesFlightSqlReader.createConnectionParameters(
                "localhost", 8815, config(true, false, "arrowflight"), null);

        Assert.assertEquals("grpc+tcp://localhost:8815", AdbcDriver.PARAM_URI.get(parameters));
    }

    @Test
    public void rejectsHostnameVerificationSkipForArrowFlight() throws Exception {
        try {
            KesFlightSqlReader.createConnectionParameters(
                    "localhost", 8815, config(true, true, ""), null);
            Assert.fail("Expected unsupported hostname verification policy to be rejected");
        } catch (KesRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("Arrow Flight"));
            Assert.assertTrue(e.getMessage().contains("skip-hostname-verification"));
        }
    }

    private static KesConfig config(
            boolean tlsEnabled, boolean skipHostnameVerification, String exclusions)
            throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put(KesOptions.KES_FENODES.getName(), "localhost:8030");
        values.put(KesOptions.KES_TABLE_IDENTIFIER.getName(), "db.tbl");
        values.put(KesOptions.KES_USER.getName(), "root");
        values.put(KesOptions.KES_PASSWORD.getName(), "secret");
        values.put(KesOptions.KES_ENABLE_TLS.getName(), Boolean.toString(tlsEnabled));
        values.put(
                KesOptions.KES_TLS_SKIP_HOSTNAME_VERIFICATION.getName(),
                Boolean.toString(skipHostnameVerification));
        values.put(KesOptions.KES_TLS_EXCLUDED_PROTOCOLS.getName(), exclusions);
        return KesConfig.fromMap(values, false);
    }
}
