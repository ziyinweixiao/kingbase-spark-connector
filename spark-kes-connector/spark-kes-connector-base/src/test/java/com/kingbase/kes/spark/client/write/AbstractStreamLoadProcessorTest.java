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

package com.kingbase.kes.spark.client.write;

import com.kingbase.kes.spark.config.KesConfig;
import com.kingbase.kes.spark.config.KesOptions;
import com.kingbase.kes.spark.rest.models.DataFormat;
import com.kingbase.kes.spark.testutil.HttpsTestServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AbstractStreamLoadProcessorTest {

    @Test
    public void getLoadTargetComputeGroup() {
        Map<String, String> properties = new HashMap<>();
        properties.put("cloud_cluster", "cluster_a");
        Assert.assertEquals("cluster_a", AbstractStreamLoadProcessor.getLoadTargetComputeGroup(properties));

        properties.put("compute_group", "cluster_b");
        Assert.assertEquals("cluster_b", AbstractStreamLoadProcessor.getLoadTargetComputeGroup(properties));
    }

    @Test
    public void streamLoadUsesConfiguredTls() throws Exception {
        String response =
                "{\"TxnId\":1,\"Label\":\"label\",\"Status\":\"Success\",\"Message\":\"OK\"}";
        try (HttpsTestServer server = new HttpsTestServer(response)) {
            Map<String, String> values =
                    HttpsTestServer.tlsConfig(
                                    server.getEndpoint("localhost"), "/tls/ca.pem", false)
                            .toMap();
            values.put(KesOptions.STREAM_LOAD_PROP_PREFIX + "group_commit", "sync_mode");
            KesConfig config = KesConfig.fromMap(values, false);
            TestStreamLoadProcessor processor = new TestStreamLoadProcessor(config);
            try {
                processor.load("row");
                Assert.assertNull(processor.stop());
            } finally {
                processor.close();
            }
        }
    }

    private static final class TestStreamLoadProcessor
            extends AbstractStreamLoadProcessor<String> {

        private TestStreamLoadProcessor(KesConfig config) throws Exception {
            super(config);
        }

        @Override
        protected String getPassThroughData(String row) {
            return row;
        }

        @Override
        public byte[] stringify(String row, DataFormat format) {
            return row.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] toArrowFormat(List<String> rows) throws IOException {
            return new byte[0];
        }

        @Override
        public String getWriteFields() {
            return "value";
        }

        @Override
        protected String generateStreamLoadLabel() {
            return "label";
        }

        @Override
        protected String copy(String row) {
            return row;
        }
    }
}
