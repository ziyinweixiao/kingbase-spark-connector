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

import com.kingbase.kes.spark.client.KesFrontendClient;
import com.kingbase.kes.spark.config.KesConfig;
import com.kingbase.kes.spark.config.KesOptions;
import com.kingbase.kes.spark.exception.KesRuntimeException;
import com.kingbase.kes.spark.exception.OptionRequiredException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

public class KesReadModeResolver {

    private static final Logger LOG = LoggerFactory.getLogger(KesReadModeResolver.class);
    private static final String ARROW = "arrow";
    private static final String THRIFT = "thrift";

    private KesReadModeResolver() {
    }

    public static String resolve(KesConfig config) {
        try {
            return resolve(config, () -> discoverPort(new KesFrontendClient(config)));
        } catch (OptionRequiredException e) {
            throw new KesRuntimeException("failed to resolve Kes read mode", e);
        }
    }

    private static int discoverPort(KesFrontendClient frontendClient) throws Exception {
        try {
            return frontendClient.tryGetArrowFlightSqlPort();
        } finally {
            frontendClient.close();
        }
    }

    static String resolve(KesConfig config, FlightSqlPortSupplier portSupplier)
            throws OptionRequiredException {
        String readMode = config.getValue(KesOptions.READ_MODE).toLowerCase(Locale.ROOT);
        if (THRIFT.equals(readMode)) {
            return THRIFT;
        }
        if (!ARROW.equals(readMode)) {
            throw new IllegalArgumentException("Unknown read mode: " + readMode);
        }

        int flightSqlPort = config.getValue(KesOptions.KES_READ_FLIGHT_SQL_PORT);
        if (flightSqlPort > 0) {
            return ARROW;
        }

        try {
            flightSqlPort = portSupplier.get();
        } catch (Exception e) {
            LOG.warn("failed to discover Arrow Flight SQL port, falling back to Thrift", e);
            return THRIFT;
        }
        if (flightSqlPort > 0) {
            config.setProperty(KesOptions.KES_READ_FLIGHT_SQL_PORT, String.valueOf(flightSqlPort));
            LOG.info("use Arrow Flight SQL to read data, port is {}", flightSqlPort);
            return ARROW;
        }

        LOG.warn("Arrow Flight SQL port {} is invalid or unavailable, falling back to Thrift", flightSqlPort);
        return THRIFT;
    }

    @FunctionalInterface
    interface FlightSqlPortSupplier {
        int get() throws Exception;
    }
}
