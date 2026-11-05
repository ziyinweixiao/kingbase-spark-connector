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

package com.kingbase.kes.spark.config;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class KesConfigStrictTest {
    private static Map<String, String> kesConfig() {
        Map<String, String> options = new HashMap<>();
        options.put("kes.fenodes", "127.0.0.1:8030");
        options.put("kes.table.identifier", "db.tbl");
        options.put("kes.user", "root");
        options.put("kes.password", "");
        return options;
    }

    @Test
    public void testKesOnlyConfiguration() throws Exception {
        KesConfig config = KesConfig.fromMap(kesConfig(), false);
        assertEquals("127.0.0.1:8030", config.getValue(KesOptions.KES_FENODES));
        assertEquals("db.tbl", config.getValue(KesOptions.KES_TABLE_IDENTIFIER));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLegacyProductPrefixIsRejected() throws Exception {
        Map<String, String> options = kesConfig();
        options.put("doris.fenodes", "127.0.0.1:8030");
        KesConfig.fromMap(options, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnprefixedOptionIsRejected() throws Exception {
        Map<String, String> options = kesConfig();
        options.put("fenodes", "127.0.0.1:8030");
        KesConfig.fromMap(options, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConflictingNormalizedKesOptionIsRejected() throws Exception {
        Map<String, String> options = kesConfig();
        options.put("kes_fenodes", "127.0.0.2:8030");
        KesConfig.fromMap(options, false);
    }

    @Test
    public void testFrameworkStreamingOptionsStayOutsideConnectorConfig() throws Exception {
        Map<String, String> options = kesConfig();
        options.put("checkpointLocation", "file:///tmp/qa-checkpoint");
        options.put("queryName", "qa-stream");
        options.put("spark.sql.shuffle.partitions", "2");
        KesConfig config = KesConfig.fromMap(options, false);
        assertEquals(kesConfig(), config.toMap());
        assertFalse(config.toMap().containsKey("kes.checkpointLocation"));
    }

    @Test
    public void testLowercaseSparkStreamingOptionsAreAccepted() throws Exception {
        Map<String, String> options = kesConfig();
        options.put("checkpointlocation", "file:///tmp/qa-checkpoint");
        options.put("queryname", "qa-stream");
        KesConfig config = KesConfig.fromMap(options, false);
        assertEquals(kesConfig(), config.toMap());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testStreamingOptionsDoNotAllowUnprefixedCredentials() throws Exception {
        Map<String, String> options = kesConfig();
        options.remove("kes.password");
        options.put("password", "not-a-secret");
        options.put("checkpointLocation", "file:///tmp/qa-checkpoint");
        KesConfig.fromMap(options, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testStreamingOptionsDoNotAllowLegacyProductPrefix() throws Exception {
        Map<String, String> options = kesConfig();
        options.put("doris.checkpointLocation", "file:///tmp/qa-checkpoint");
        KesConfig.fromMap(options, false);
    }
}
