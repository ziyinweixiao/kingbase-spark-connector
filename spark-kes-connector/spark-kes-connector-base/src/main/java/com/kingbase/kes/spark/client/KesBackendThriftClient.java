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

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.sdk.thrift.TDorisExternalService;
import org.apache.doris.sdk.thrift.TScanBatchResult;
import org.apache.doris.sdk.thrift.TScanCloseParams;
import org.apache.doris.sdk.thrift.TScanCloseResult;
import org.apache.doris.sdk.thrift.TScanNextBatchParams;
import org.apache.doris.sdk.thrift.TScanOpenParams;
import org.apache.doris.sdk.thrift.TScanOpenResult;
import org.apache.doris.sdk.thrift.TStatusCode;
import com.kingbase.kes.spark.client.entity.Backend;
import com.kingbase.kes.spark.config.KesConfig;
import com.kingbase.kes.spark.config.KesOptions;
import com.kingbase.kes.spark.exception.ConnectedFailedException;
import com.kingbase.kes.spark.exception.KesException;
import com.kingbase.kes.spark.exception.KesInternalException;
import com.kingbase.kes.spark.exception.OptionRequiredException;
import com.kingbase.kes.spark.util.ErrorMessages;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client to request Kes BE
 */
public class KesBackendThriftClient {

    private final static Logger logger = LoggerFactory.getLogger(KesBackendThriftClient.class);

    private final Backend backend;
    private final KesConfig config;

    private TDorisExternalService.Client client;
    private TTransport transport;

    private boolean isConnected = false;
    private final int retries;

    public KesBackendThriftClient(Backend backend, KesConfig config) throws ConnectedFailedException, OptionRequiredException {
        this.backend = backend;
        this.config = config;
        this.retries = config.getValue(KesOptions.KES_REQUEST_RETRIES);
        open();
    }

    private void open() throws ConnectedFailedException {
        logger.debug("Open client to Kes BE '{}'.", backend);
        Exception ex = null;
        for (int attempt = 0; !isConnected && attempt < retries; ++attempt) {
            logger.debug("Attempt {} to connect {}.", attempt, backend);
            try {
                TBinaryProtocol.Factory factory = new TBinaryProtocol.Factory();
                Integer connectTimeout = config.getValue(KesOptions.KES_REQUEST_CONNECT_TIMEOUT_MS);
                Integer socketTimeout = config.getValue(KesOptions.KES_REQUEST_READ_TIMEOUT_MS);
                logger.trace("connect timeout set to '{}'. socket timeout set to '{}'. retries set to '{}'.",
                        connectTimeout, socketTimeout, this.retries);
                transport = KesThriftTransportFactory.create(backend, config);
                TProtocol protocol = factory.getProtocol(transport);
                client = new TDorisExternalService.Client(protocol);
                logger.trace("Connect status before open transport to {} is '{}'.", backend, isConnected);
                if (!transport.isOpen()) {
                    transport.open();
                    isConnected = true;
                }
            } catch (TTransportException e) {
                logger.warn(ErrorMessages.CONNECT_FAILED_MESSAGE, backend, e);
                ex = e;
            } catch (OptionRequiredException e) {
                ex = e;
            }
            if (isConnected) {
                logger.info("Success connect to {}.", backend);
                break;
            }
        }
        if (!isConnected) {
            logger.error(ErrorMessages.CONNECT_FAILED_MESSAGE, backend);
            throw new ConnectedFailedException(backend.toString(), ex);
        }
    }

    private void close() {
        logger.trace("Connect status before close with '{}' is '{}'.", backend, isConnected);
        isConnected = false;
        if (null != client) {
            client = null;
        }
        if ((transport != null) && transport.isOpen()) {
            transport.close();
            logger.info("Closed a connection to {}.", backend);
        }
    }

    /**
     * Open a scanner for reading Kes data.
     * @param openParams thrift struct to required by request
     * @return scan open result
     * @throws ConnectedFailedException throw if cannot connect to Kes BE
     */
    public TScanOpenResult openScanner(TScanOpenParams openParams) throws ConnectedFailedException {
        logger.info("OpenScanner to '{}' tablets: [{}].", backend, StringUtils.join(openParams.tablet_ids, ","));
        if (logger.isDebugEnabled()) {
            TScanOpenParams logParams = new TScanOpenParams(openParams);
            logParams.setPasswd("********");
            logger.debug("OpenScanner to '{}', parameter is '{}'.", backend, logParams);
        }
        if (!isConnected) {
            open();
        }
        TException ex = null;
        for (int attempt = 0; attempt < retries; ++attempt) {
            logger.debug("Attempt {} to openScanner {}.", attempt, backend);
            try {
                TScanOpenResult result = client.openScanner(openParams);
                if (result == null) {
                    logger.warn("Open scanner result from {} is null.", backend);
                    continue;
                }
                if (!TStatusCode.OK.equals(result.getStatus().getStatusCode())) {
                    logger.warn("The status of open scanner result from {} is '{}', error message is: {}.",
                            backend, result.getStatus().getStatusCode(), result.getStatus().getErrorMsgs());
                    continue;
                }
                logger.info("OpenScanner to '{}' tablets: [{}] success, context id: {}.", backend,
                        StringUtils.join(openParams.tablet_ids, ","), result.getContextId());
                return result;
            } catch (TException e) {
                logger.warn("Open scanner from {} failed.", backend, e);
                ex = e;
            }
        }
        logger.error(ErrorMessages.CONNECT_FAILED_MESSAGE, backend);
        throw new ConnectedFailedException(backend.toString(), ex);
    }

    /**
     * get next row batch from Kes BE
     * @param nextBatchParams thrift struct to required by request
     * @return scan batch result
     * @throws ConnectedFailedException throw if cannot connect to Kes BE
     */
    public TScanBatchResult getNext(TScanNextBatchParams nextBatchParams) throws KesException {
        logger.debug("GetNext to '{}', parameter is '{}'.", backend, nextBatchParams);
        if (!isConnected) {
            open();
        }
        TException ex = null;
        TScanBatchResult result = null;
        for (int attempt = 0; attempt < retries; ++attempt) {
            logger.debug("Attempt {} to getNext {}.", attempt, backend);
            try {
                result  = client.getNext(nextBatchParams);
                if (result == null) {
                    logger.warn("GetNext result from {} is null.", backend);
                    continue;
                }
                if (!TStatusCode.OK.equals(result.getStatus().getStatusCode())) {
                    logger.warn("The status of get next result from {} is '{}', error message is: {}.",
                            backend, result.getStatus().getStatusCode(), result.getStatus().getErrorMsgs());
                    continue;
                }
                return result;
            } catch (TException e) {
                logger.warn("Get next from {} failed.", backend, e);
                ex = e;
            }
        }
        if (result != null && (TStatusCode.OK != (result.getStatus().getStatusCode()))) {
            logger.error(ErrorMessages.KES_INTERNAL_FAIL_MESSAGE, backend, result.getStatus().getStatusCode(),
                    result.getStatus().getErrorMsgs());
            throw new KesInternalException(backend.toString(), result.getStatus().getStatusCode(),
                    result.getStatus().getErrorMsgs());
        }
        logger.error(ErrorMessages.CONNECT_FAILED_MESSAGE, backend);
        throw new ConnectedFailedException(backend.toString(), ex);
    }

    /**
     * close an scanner.
     * @param closeParams thrift struct to required by request
     */
    public void closeScanner(TScanCloseParams closeParams) {
        logger.debug("CloseScanner to '{}', parameter is '{}'.", backend, closeParams);
        if (!isConnected) {
            try {
                open();
            } catch (ConnectedFailedException e) {
                logger.warn("Cannot connect to Kes BE {} when close scanner.", backend);
                return;
            }
        }
        for (int attempt = 0; attempt < retries; ++attempt) {
            logger.debug("Attempt {} to closeScanner {}.", attempt, backend);
            try {
                TScanCloseResult result = client.closeScanner(closeParams);
                if (result == null) {
                    logger.warn("CloseScanner result from {} is null.", backend);
                    continue;
                }
                if (!TStatusCode.OK.equals(result.getStatus().getStatusCode())) {
                    logger.warn("The status of get next result from {} is '{}', error message is: {}.",
                            backend, result.getStatus().getStatusCode(), result.getStatus().getErrorMsgs());
                    continue;
                }
                break;
            } catch (TException e) {
                logger.warn("Close scanner from {} failed.", backend, e);
            }
        }
        logger.info("CloseScanner to Kes BE '{}' success or contextId {} .", backend, closeParams.getContextId());
        close();
    }
}
