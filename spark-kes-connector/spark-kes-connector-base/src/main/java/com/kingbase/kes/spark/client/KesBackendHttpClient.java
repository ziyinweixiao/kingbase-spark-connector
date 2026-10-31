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
import com.kingbase.kes.spark.config.KesTlsOptions;
import com.kingbase.kes.spark.util.HttpUtil;
import com.kingbase.kes.spark.util.HttpUtils;
import com.kingbase.kes.spark.util.LoadBalanceList;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.function.BiFunction;

public class KesBackendHttpClient implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(KesBackendHttpClient.class);

    private final LoadBalanceList<Backend> backends;
    private final KesConfig config;
    private final KesTlsOptions tlsOptions;

    private transient CloseableHttpClient httpClient;

    public KesBackendHttpClient(List<Backend> backends) {
        this.backends = new LoadBalanceList<>(backends);
        this.config = null;
        this.tlsOptions = null;
    }

    public KesBackendHttpClient(List<Backend> backends, KesConfig config) throws Exception {
        this.backends = new LoadBalanceList<>(backends);
        this.config = config;
        this.tlsOptions = config.getTlsOptions();
    }

    public <T> T executeReq(BiFunction<Backend, CloseableHttpClient, T> reqFunc) throws Exception {
        if (httpClient == null) {
            httpClient = config == null ? HttpClients.createDefault() : HttpUtils.getHttpClient(config);
        }
        Exception ex = null;
        for (Backend backend : backends) {
            try {
                boolean reachable = HttpUtil.tryHttpConnection(
                        backend.hostHttpPortString(), tlsOptions);
                if (reachable) {
                    return reqFunc.apply(backend, httpClient);
                }
            } catch (Exception e) {
                log.warn("Failed to execute request on backend: {}:{}", backend.getHost(), backend.getHttpPort(), e);
                backends.reportFailed(backend);
                ex = e;
            }
        }

        if (ex == null) {
            ex = new Exception("All backends failed to execute request.");
        }
        throw ex;
    }


    public void close() {
        if (httpClient != null) {
            try {
                httpClient.close();
            } catch (IOException e) {
                log.warn("Failed to close http client", e);
            }
        }
    }

}
