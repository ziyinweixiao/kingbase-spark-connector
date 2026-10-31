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

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

public class KesContainer implements ContainerService {
    private static final Logger LOG = LoggerFactory.getLogger(KesContainer.class);
    private static final String KES_DOCKER_IMAGE = requireKesQaImage();

    private static String requireKesQaImage() {
        String image = System.getProperty("image");
        if (image == null || image.trim().isEmpty()) {
            throw new IllegalStateException("KES QA Docker image must be set via -Dimage");
        }
        return image;
    }
    private static final String DRIVER_JAR =
            "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.16/mysql-connector-java-8.0.16.jar";
    private static final String JDBC_URL = "jdbc:mysql://%s:9030";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";
    private final GenericContainer kesContainer;

    public KesContainer() {
        kesContainer = createKesContainer();
    }

    public GenericContainer createKesContainer() {
        LOG.info("Will create kes containers.");
        GenericContainer container =
                new GenericContainer<>(KES_DOCKER_IMAGE)
                        .withNetwork(Network.newNetwork())
                        .withNetworkAliases("KesContainer")
                        .withPrivilegedMode(true)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(KES_DOCKER_IMAGE)))
                        .withExposedPorts(8030, 9030, 8040, 9060, 9611, 9610);

        container.setPortBindings(
                Lists.newArrayList(
                        String.format("%s:%s", "8030", "8030"),
                        String.format("%s:%s", "9030", "9030"),
                        String.format("%s:%s", "9060", "9060"),
                        String.format("%s:%s", "8040", "8040"),
                        String.format("%s:%s", "9611", "9611"),
                        String.format("%s:%s", "9610", "9610")));
        return container;
    }

    public void startContainer() {
        try {
            LOG.info("Starting kes containers.");
            // singleton kes container
            kesContainer.start();
            initializeJdbcConnection();
            initializeVariables();
            printClusterStatus();
        } catch (Exception ex) {
            LOG.error("Failed to start containers kes", ex);
            throw new KesRuntimeException("Failed to start containers kes", ex);
        }
        LOG.info("Kes container started successfully.");
    }

    @Override
    public void restartContainer() {
        LOG.info("Restart kes container.");
        kesContainer
                .getDockerClient()
                .restartContainerCmd(kesContainer.getContainerId())
                .exec();
    }

    @Override
    public boolean isRunning() {
        return kesContainer.isRunning();
    }

    @Override
    public Connection getQueryConnection() {
       return getQueryConnection("");
    }

    @Override
    public Connection getQueryConnection(String database) {
        LOG.info("Try to get query connection from kes.");
        String jdbcUrl = String.format(JDBC_URL, kesContainer.getHost());
        jdbcUrl = jdbcUrl + "/" + database;
        try {
            return DriverManager.getConnection(jdbcUrl, USERNAME, PASSWORD);
        } catch (SQLException e) {
            LOG.info("Failed to get kes query connection. jdbcUrl={}", jdbcUrl, e);
            throw new KesRuntimeException(e);
        }
    }

    private void initializeVariables() throws Exception {
        try (Connection connection = getQueryConnection();
                Statement statement = connection.createStatement()) {
            LOG.info("init kes cluster variables.");
            // avoid arrow flight sql reading bug
            statement.execute("SET PROPERTY FOR 'root' 'max_user_connections' = '1024';");
        }
        LOG.info("Init variables successfully.");
    }

    @Override
    public String getJdbcUrl() {
        return String.format(JDBC_URL, kesContainer.getHost());
    }

    @Override
    public String getInstanceHost() {
        return kesContainer.getHost();
    }

    @Override
    public Integer getMappedPort(int originalPort) {
        return kesContainer.getMappedPort(originalPort);
    }

    @Override
    public String getUsername() {
        return USERNAME;
    }

    @Override
    public String getPassword() {
        return PASSWORD;
    }

    @Override
    public String getFenodes() {
        return kesContainer.getHost() + ":8030";
    }

    @Override
    public String getBenodes() {
        return kesContainer.getHost() + ":8040";
    }

    public void close() {
        LOG.info("Kes container is about to be close.");
        kesContainer.close();
        LOG.info("Kes container closed successfully.");
    }

    @Override
    public int getQueryPort() {
        return 9030;
    }

    private void initializeJDBCDriver() throws MalformedURLException {
        URLClassLoader urlClassLoader =
                new URLClassLoader(
                        new URL[] {new URL(DRIVER_JAR)}, KesContainer.class.getClassLoader());
        LOG.info("Try to connect to Kes.");
        Thread.currentThread().setContextClassLoader(urlClassLoader);
    }

    private void initializeJdbcConnection() throws Exception {
        initializeJDBCDriver();
        Duration timeout = Duration.ofMinutes(5L);
        long startNanos = System.nanoTime();
        Throwable lastFailure = null;
        boolean frontendReady = false;
        while (System.nanoTime() - startNanos < timeout.toNanos()) {
            try (Connection connection = getQueryConnection();
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("show backends")) {
                frontendReady = true;
                lastFailure = null;
                LOG.info("Waiting for the Backend to start successfully.");
                if (isBeReady(resultSet, Duration.ofSeconds(1L))) {
                    LOG.info("Connected to Kes successfully.");
                    return;
                }
            } catch (KesRuntimeException | SQLException e) {
                frontendReady = false;
                lastFailure = e;
                LOG.info("Waiting for the Frontend to accept connections.");
                LockSupport.parkNanos(Duration.ofSeconds(1L).toNanos());
            }
        }
        throw new KesRuntimeException(
                String.format(
                        "Kes %s did not become ready within %d seconds.",
                        frontendReady ? "Backend" : "Frontend", timeout.getSeconds()),
                lastFailure);
    }

    private boolean isBeReady(ResultSet rs, Duration duration) throws SQLException {
        LockSupport.parkNanos(duration.toNanos());
        if (rs.next()) {
            String isAlive = rs.getString("Alive").trim();
            String totalCap = rs.getString("TotalCapacity").trim();
            return Boolean.toString(true).equalsIgnoreCase(isAlive)
                    && !"0.000".equalsIgnoreCase(totalCap);
        }
        return false;
    }

    private void printClusterStatus() throws Exception {
        LOG.info("Current machine IP: {}", kesContainer.getHost());
        echo("sh", "-c", "cat /proc/cpuinfo | grep 'cpu cores' | uniq");
        echo("sh", "-c", "free -h");
        try (Connection connection =
                        DriverManager.getConnection(
                                String.format(JDBC_URL, kesContainer.getHost()),
                                USERNAME,
                                PASSWORD);
                Statement statement = connection.createStatement()) {
            ResultSet showFrontends = statement.executeQuery("show frontends");
            LOG.info("Frontends status: {}", convertList(showFrontends));
            ResultSet showBackends = statement.executeQuery("show backends");
            LOG.info("Backends status: {}", convertList(showBackends));
        }
    }

    private void echo(String... cmd) {
        try {
            Process p = Runtime.getRuntime().exec(cmd);
            InputStream is = p.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
            p.waitFor();
            is.close();
            reader.close();
            p.destroy();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<Map> convertList(ResultSet rs) throws SQLException {
        List<Map> list = new ArrayList<>();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (rs.next()) {
            Map<String, Object> rowData = new HashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                rowData.put(metaData.getColumnName(i), rs.getObject(i));
            }
            list.add(rowData);
        }
        return list;
    }
}
