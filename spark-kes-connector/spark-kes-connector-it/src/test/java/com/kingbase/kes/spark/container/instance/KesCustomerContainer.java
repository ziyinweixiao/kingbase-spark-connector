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

package com.kingbase.kes.spark.container.instance;

import com.kingbase.kes.spark.exception.KesRuntimeException;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/** Using a custom Kes environment */
public class KesCustomerContainer implements ContainerService {
    private static final Logger LOG = LoggerFactory.getLogger(KesCustomerContainer.class);
    private static final String JDBC_URL = "jdbc:mysql://%s:%s";

    @Override
    public void startContainer() {
        LOG.info("Using kes customer containers env.");
        checkParams();
        if (!isRunning()) {
            throw new KesRuntimeException(
                    "Backend is not alive. Please check the kes cluster.");
        }
    }

    private void checkParams() {
        Preconditions.checkArgument(
                System.getProperty("kes_host") != null, "kes_host is required.");
        Preconditions.checkArgument(
                System.getProperty("kes_query_port") != null, "kes_query_port is required.");
        Preconditions.checkArgument(
                System.getProperty("kes_http_port") != null, "kes_http_port is required.");
        Preconditions.checkArgument(
                System.getProperty("kes_user") != null, "kes_user is required.");
        Preconditions.checkArgument(
                System.getProperty("kes_passwd") != null, "kes_passwd is required.");
    }

    @Override
    public boolean isRunning() {
        try (Connection conn = getQueryConnection();
                Statement stmt = conn.createStatement()) {
            ResultSet showBackends = stmt.executeQuery("show backends");
            while (showBackends.next()) {
                String isAlive = showBackends.getString("Alive").trim();
                if (Boolean.toString(true).equalsIgnoreCase(isAlive)) {
                    return true;
                }
            }
        } catch (SQLException e) {
            LOG.error("Failed to connect kes cluster.", e);
            return false;
        }
        return false;
    }

    @Override
    public Connection getQueryConnection() {
        return getQueryConnection("");
    }

    @Override
    public Connection getQueryConnection(String database) {
        LOG.info("Try to get query connection from kes.");
        String jdbcUrl =
                String.format(
                        JDBC_URL,
                        System.getProperty("kes_host"),
                        System.getProperty("kes_query_port"));
        jdbcUrl = jdbcUrl + "/" + database;
        try {
            return DriverManager.getConnection(jdbcUrl, getUsername(), getPassword());
        } catch (SQLException e) {
            LOG.info("Failed to get kes query connection. jdbcUrl={}", jdbcUrl, e);
            throw new KesRuntimeException(e);
        }
    }

    @Override
    public String getJdbcUrl() {
        return String.format(
                JDBC_URL, System.getProperty("kes_host"), System.getProperty("kes_query_port"));
    }

    @Override
    public String getInstanceHost() {
        return System.getProperty("kes_host");
    }

    @Override
    public Integer getMappedPort(int originalPort) {
        return originalPort;
    }

    @Override
    public String getUsername() {
        return System.getProperty("kes_user");
    }

    @Override
    public String getPassword() {
        return System.getProperty("kes_passwd");
    }

    @Override
    public String getFenodes() {
        return System.getProperty("kes_host") + ":" + System.getProperty("kes_http_port");
    }

    @Override
    public String getBenodes() {
        return null;
    }

    @Override
    public void close() {}

    @Override
    public int getQueryPort() {
        return Integer.valueOf(System.getProperty("kes_query_port"));
    }
}
