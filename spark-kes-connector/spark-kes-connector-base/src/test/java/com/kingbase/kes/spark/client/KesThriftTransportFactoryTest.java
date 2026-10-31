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

package com.kingbase.kes.spark.client;

import com.kingbase.kes.spark.client.entity.Backend;
import com.kingbase.kes.spark.config.KesConfig;
import com.kingbase.kes.spark.config.KesOptions;
import com.kingbase.kes.spark.testutil.HttpsTestServer;

import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class KesThriftTransportFactoryTest {

    @Test
    public void createsPlainSocketWhenTlsIsDisabled() throws Exception {
        TTransport transport = KesThriftTransportFactory.create(
                new Backend("localhost", 9030), config(false, "", ""));

        Assert.assertTrue(transport instanceof TSocket);
    }

    @Test
    public void createsPlainSocketWhenThriftIsExcluded() throws Exception {
        TTransport transport = KesThriftTransportFactory.create(
                new Backend("localhost", 9030),
                config(true, "/tls/ca.pem", "thrift"));

        Assert.assertTrue(transport instanceof TSocket);
    }

    @Test
    public void opensTlsTransportWithConfiguredCa() throws Exception {
        try (HttpsTestServer server = new HttpsTestServer()) {
            TTransport transport = KesThriftTransportFactory.create(
                    new Backend("localhost", server.getPort()),
                    config(true, "/tls/ca.pem", ""));
            try {
                transport.open();
                Assert.assertTrue(transport.isOpen());
            } finally {
                transport.close();
            }
        }
    }

    @Test
    public void rejectsTlsServerSignedByDifferentCa() throws Exception {
        try (HttpsTestServer server = new HttpsTestServer()) {
            TTransport transport = KesThriftTransportFactory.create(
                    new Backend("localhost", server.getPort()),
                    config(true, "/tls/wrong-ca.pem", ""));
            try {
                transport.open();
                Assert.fail("Expected the TLS handshake to reject the server certificate");
            } catch (TTransportException expected) {
                Assert.assertNotNull(expected.getCause());
            } finally {
                transport.close();
            }
        }
    }

    private static KesConfig config(boolean tlsEnabled, String caResource, String exclusions)
            throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put(KesOptions.KES_FENODES.getName(), "localhost:8030");
        values.put(KesOptions.KES_TABLE_IDENTIFIER.getName(), "db.tbl");
        values.put(KesOptions.KES_USER.getName(), "root");
        values.put(KesOptions.KES_PASSWORD.getName(), "");
        values.put(KesOptions.KES_ENABLE_TLS.getName(), Boolean.toString(tlsEnabled));
        values.put(
                KesOptions.KES_TLS_CA_CERTIFICATE_PATH.getName(),
                caResource.isEmpty() ? "" : HttpsTestServer.resourcePath(caResource));
        values.put(KesOptions.KES_TLS_EXCLUDED_PROTOCOLS.getName(), exclusions);
        values.put(KesOptions.KES_REQUEST_CONNECT_TIMEOUT_MS.getName(), "2000");
        values.put(KesOptions.KES_REQUEST_READ_TIMEOUT_MS.getName(), "2000");
        return KesConfig.fromMap(values, false);
    }
}
