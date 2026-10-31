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
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class KesReadModeResolverTest {

    @Test
    public void readOptionsDefaultToArrowWithInvalidFlightSqlPort() {
        Assert.assertEquals("arrow", KesOptions.READ_MODE.getDefaultValue());
        Assert.assertEquals(Integer.valueOf(-1), KesOptions.KES_READ_FLIGHT_SQL_PORT.getDefaultValue());
    }

    @Test
    public void defaultArrowModeDiscoversFlightSqlPort() throws Exception {
        KesConfig config = createConfig(null, null);

        String mode = KesReadModeResolver.resolve(config, () -> 9040);

        Assert.assertEquals("arrow", mode);
        Assert.assertEquals(Integer.valueOf(9040), config.getValue(KesOptions.KES_READ_FLIGHT_SQL_PORT));
    }

    @Test
    public void explicitThriftSkipsPortDiscovery() throws Exception {
        KesConfig config = createConfig("thrift", null);
        AtomicBoolean discovered = new AtomicBoolean(false);

        String mode = KesReadModeResolver.resolve(config, () -> {
            discovered.set(true);
            return 9040;
        });

        Assert.assertEquals("thrift", mode);
        Assert.assertFalse(discovered.get());
    }

    @Test
    public void configuredPositivePortKeepsArrowMode() throws Exception {
        KesConfig config = createConfig("arrow", 9040);
        AtomicBoolean discovered = new AtomicBoolean(false);

        String mode = KesReadModeResolver.resolve(config, () -> {
            discovered.set(true);
            return 9050;
        });

        Assert.assertEquals("arrow", mode);
        Assert.assertFalse(discovered.get());
    }

    @Test
    public void discoveredPositivePortKeepsArrowModeAndUpdatesConfig() throws Exception {
        KesConfig config = createConfig("arrow", null);

        String mode = KesReadModeResolver.resolve(config, () -> 9040);

        Assert.assertEquals("arrow", mode);
        Assert.assertEquals(Integer.valueOf(9040), config.getValue(KesOptions.KES_READ_FLIGHT_SQL_PORT));
    }

    @Test
    public void invalidDiscoveredPortFallsBackToThrift() throws Exception {
        KesConfig config = createConfig("arrow", null);

        String mode = KesReadModeResolver.resolve(config, () -> -1);

        Assert.assertEquals("thrift", mode);
    }

    @Test
    public void unknownReadModeIsRejected() throws Exception {
        KesConfig config = createConfig("unknown", null);

        try {
            KesReadModeResolver.resolve(config, () -> 9040);
            Assert.fail("Expected unknown read mode to be rejected");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("Unknown read mode: unknown", e.getMessage());
        }
    }

    private KesConfig createConfig(String readMode, Integer flightSqlPort) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("kes.fenodes", "127.0.0.1:8030");
        options.put("kes.table.identifier", "db.tbl");
        options.put("kes.user", "root");
        options.put("kes.password", "");
        if (readMode != null) {
            options.put("kes.read.mode", readMode);
        }
        if (flightSqlPort != null) {
            options.put("kes.read.arrow-flight-sql.port", flightSqlPort.toString());
        }
        return KesConfig.fromMap(options, false);
    }

}
