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

package com.kingbase.kes.spark.container;

import com.kingbase.kes.spark.config.KesConfig;
import com.kingbase.kes.spark.config.KesOptions;
import com.kingbase.kes.spark.exception.OptionRequiredException;
import com.kingbase.kes.spark.util.KesJdbcTlsAdapter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/** External Kes TLS settings shared by the opt-in end-to-end test. */
public final class ExternalKesTlsTestEnvironment {

    private static final String TLS_TEST = "kes_tls_test";
    private static final String CUSTOMER_ENV = "customer_env";
    private static final String HOST = "kes_host";
    private static final String QUERY_PORT = "kes_query_port";
    private static final String HTTP_PORT = "kes_http_port";
    private static final String USER = "kes_user";
    private static final String PASSWORD = "kes_passwd";
    private static final String CA_CERTIFICATE_PATH = "kes_tls_ca_certificate_path";
    private static final String SKIP_HOSTNAME_VERIFICATION =
            "kes_tls_skip_hostname_verification";
    private static final String EXCLUDED_PROTOCOLS = "kes_tls_excluded_protocols";
    private static final String FLIGHT_SQL_PORT = "kes_flight_sql_port";

    private final String host;
    private final int queryPort;
    private final int httpPort;
    private final String user;
    private final String password;
    private final String caCertificatePath;
    private final boolean skipHostnameVerification;
    private final String excludedProtocols;
    private final int flightSqlPort;

    private ExternalKesTlsTestEnvironment(
            String host,
            int queryPort,
            int httpPort,
            String user,
            String password,
            String caCertificatePath,
            boolean skipHostnameVerification,
            String excludedProtocols,
            int flightSqlPort) {
        this.host = host;
        this.queryPort = queryPort;
        this.httpPort = httpPort;
        this.user = user;
        this.password = password;
        this.caCertificatePath = caCertificatePath;
        this.skipHostnameVerification = skipHostnameVerification;
        this.excludedProtocols = excludedProtocols;
        this.flightSqlPort = flightSqlPort;
    }

    public static boolean isEnabled(Properties properties) {
        String configured = properties.getProperty(TLS_TEST);
        if (configured == null || configured.trim().isEmpty()) {
            return false;
        }
        return parseBoolean(TLS_TEST, configured);
    }

    public static ExternalKesTlsTestEnvironment fromSystemProperties() {
        return fromProperties(System.getProperties());
    }

    public static ExternalKesTlsTestEnvironment fromProperties(Properties properties) {
        if (!isEnabled(properties)) {
            throw new IllegalArgumentException(TLS_TEST + " must be true");
        }
        if (!parseBoolean(CUSTOMER_ENV, requireNonEmpty(properties, CUSTOMER_ENV))) {
            throw new IllegalArgumentException(CUSTOMER_ENV + " must be true");
        }

        return new ExternalKesTlsTestEnvironment(
                requireNonEmpty(properties, HOST),
                parsePort(properties, QUERY_PORT),
                parsePort(properties, HTTP_PORT),
                requireNonEmpty(properties, USER),
                requirePresent(properties, PASSWORD),
                requireNonEmpty(properties, CA_CERTIFICATE_PATH),
                parseOptionalBoolean(properties, SKIP_HOSTNAME_VERIFICATION, false),
                properties.getProperty(EXCLUDED_PROTOCOLS, "").trim(),
                parsePort(properties, FLIGHT_SQL_PORT));
    }

    public Map<String, String> connectorOptions(String tableIdentifier, String readMode) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put(KesOptions.KES_FENODES.getName(), host + ":" + httpPort);
        options.put(KesOptions.KES_TABLE_IDENTIFIER.getName(), tableIdentifier);
        options.put(KesOptions.KES_USER.getName(), user);
        options.put(KesOptions.KES_PASSWORD.getName(), password);
        options.put(KesOptions.KES_ENABLE_TLS.getName(), "true");
        options.put(KesOptions.KES_TLS_CA_CERTIFICATE_PATH.getName(), caCertificatePath);
        options.put(
                KesOptions.KES_TLS_SKIP_HOSTNAME_VERIFICATION.getName(),
                Boolean.toString(skipHostnameVerification));
        options.put(KesOptions.KES_TLS_EXCLUDED_PROTOCOLS.getName(), excludedProtocols);

        if (readMode != null && !readMode.trim().isEmpty()) {
            String normalizedReadMode = readMode.trim().toLowerCase(java.util.Locale.ROOT);
            if (!"thrift".equals(normalizedReadMode) && !"arrow".equals(normalizedReadMode)) {
                throw new IllegalArgumentException("Unsupported Kes read mode: " + readMode);
            }
            options.put(KesOptions.READ_MODE.getName(), normalizedReadMode);
            if ("arrow".equals(normalizedReadMode)) {
                options.put(
                        KesOptions.KES_READ_FLIGHT_SQL_PORT.getName(),
                        Integer.toString(flightSqlPort));
            }
        }
        return options;
    }

    public Connection openConnection() throws SQLException {
        return openConnection("");
    }

    public Connection openConnection(String database) throws SQLException {
        String jdbcUrl = "jdbc:mysql://" + host + ":" + queryPort;
        if (database != null && !database.isEmpty()) {
            jdbcUrl += "/" + database;
        }

        try {
            KesConfig config =
                    KesConfig.fromMap(
                            connectorOptions("information_schema.tables", "thrift"), false);
            try (KesJdbcTlsAdapter adapter =
                    KesJdbcTlsAdapter.create(config.getTlsOptions())) {
                adapter.validateJdbcUrl(jdbcUrl);
                return DriverManager.getConnection(
                        jdbcUrl, adapter.createConnectionProperties(user, password));
            }
        } catch (OptionRequiredException e) {
            throw new SQLException("Invalid external Kes TLS configuration", e);
        }
    }

    private static String requirePresent(Properties properties, String name) {
        String value = properties.getProperty(name);
        if (value == null) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }

    private static String requireNonEmpty(Properties properties, String name) {
        String value = requirePresent(properties, name).trim();
        if (value.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return value;
    }

    private static int parsePort(Properties properties, String name) {
        String configured = requireNonEmpty(properties, name);
        try {
            int port = Integer.parseInt(configured);
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException(name + " must be between 1 and 65535");
            }
            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be an integer", e);
        }
    }

    private static boolean parseOptionalBoolean(
            Properties properties, String name, boolean defaultValue) {
        String configured = properties.getProperty(name);
        return configured == null ? defaultValue : parseBoolean(name, configured);
    }

    private static boolean parseBoolean(String name, String configured) {
        String normalized = configured.trim();
        if ("true".equalsIgnoreCase(normalized)) {
            return true;
        }
        if ("false".equalsIgnoreCase(normalized)) {
            return false;
        }
        throw new IllegalArgumentException(name + " must be true or false");
    }
}
