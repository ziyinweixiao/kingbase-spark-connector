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

import com.kingbase.kes.spark.config.KesTlsOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

/** Adapts the connector TLS policy to MySQL Connector/J connection properties. */
public final class KesJdbcTlsAdapter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KesJdbcTlsAdapter.class);
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final Set<String> MANAGED_PROPERTIES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(
                    "sslmode",
                    "usessl",
                    "requiressl",
                    "verifyservercertificate",
                    "trustcertificatekeystoreurl",
                    "trustcertificatekeystoretype",
                    "trustcertificatekeystorepassword")));

    private final KesTlsOptions tlsOptions;
    private final Path trustStorePath;
    private final String trustStorePassword;

    private KesJdbcTlsAdapter(
            KesTlsOptions tlsOptions, Path trustStorePath, String trustStorePassword) {
        this.tlsOptions = tlsOptions;
        this.trustStorePath = trustStorePath;
        this.trustStorePassword = trustStorePassword;
    }

    public static KesJdbcTlsAdapter create(KesTlsOptions tlsOptions) throws SQLException {
        if (!tlsOptions.isEnabledFor(KesTlsOptions.Protocol.MYSQL)
                || tlsOptions.getCaCertificatePath().isEmpty()) {
            return new KesJdbcTlsAdapter(tlsOptions, null, null);
        }

        Path trustStorePath = null;
        try {
            KeyStore source =
                    KesTlsContextFactory.createTrustStore(tlsOptions.getCaCertificatePath());
            KeyStore target = KeyStore.getInstance("PKCS12");
            target.load(null, null);
            Enumeration<String> aliases = source.aliases();
            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                Certificate certificate = source.getCertificate(alias);
                if (certificate != null) {
                    target.setCertificateEntry(alias, certificate);
                }
            }

            String password = randomPassword();
            trustStorePath = Files.createTempFile("kes-jdbc-trust-", ".p12");
            try (OutputStream output = Files.newOutputStream(trustStorePath)) {
                target.store(output, password.toCharArray());
            }
            return new KesJdbcTlsAdapter(tlsOptions, trustStorePath, password);
        } catch (Exception e) {
            deleteIfExists(trustStorePath);
            throw new SQLException("Unable to create the temporary Kes JDBC truststore", e);
        }
    }

    public Properties createConnectionProperties(String username, String password) {
        Properties properties = new Properties();
        if (username != null) {
            properties.setProperty("user", username);
        }
        if (password != null) {
            properties.setProperty("password", password);
        }
        if (!tlsOptions.isEnabledFor(KesTlsOptions.Protocol.MYSQL)) {
            return properties;
        }

        properties.setProperty(
                "sslMode", tlsOptions.isSkipHostnameVerification() ? "VERIFY_CA" : "VERIFY_IDENTITY");
        if (trustStorePath != null) {
            properties.setProperty("trustCertificateKeyStoreUrl", trustStorePath.toUri().toString());
            properties.setProperty("trustCertificateKeyStoreType", "PKCS12");
            properties.setProperty("trustCertificateKeyStorePassword", trustStorePassword);
        }
        return properties;
    }

    public void validateJdbcUrl(String jdbcUrl) throws SQLException {
        if (!tlsOptions.isEnabledFor(KesTlsOptions.Protocol.MYSQL)) {
            return;
        }
        int queryStart = jdbcUrl.indexOf('?');
        if (queryStart < 0 || queryStart == jdbcUrl.length() - 1) {
            return;
        }
        String[] parameters = jdbcUrl.substring(queryStart + 1).split("&");
        for (String parameter : parameters) {
            String name = parameter.split("=", 2)[0].trim();
            if (MANAGED_PROPERTIES.contains(name.toLowerCase(Locale.ROOT))) {
                throw new SQLException(
                        "JDBC URL property '" + name
                                + "' conflicts with the Kes connector TLS configuration");
            }
        }
    }

    @Override
    public void close() {
        deleteIfExists(trustStorePath);
    }

    private static String randomPassword() {
        byte[] bytes = new byte[24];
        RANDOM.nextBytes(bytes);
        StringBuilder password = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            password.append(String.format(Locale.ROOT, "%02x", value & 0xff));
        }
        return password.toString();
    }

    private static void deleteIfExists(Path path) {
        if (path == null) {
            return;
        }
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            LOG.warn("Unable to delete temporary Kes JDBC truststore {}", path, e);
        }
    }
}
