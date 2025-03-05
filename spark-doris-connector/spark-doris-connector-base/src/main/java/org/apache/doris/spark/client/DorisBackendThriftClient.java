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

package org.apache.doris.spark.client;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.sdk.thrift.TDorisExternalService;
import org.apache.doris.sdk.thrift.TScanBatchResult;
import org.apache.doris.sdk.thrift.TScanCloseParams;
import org.apache.doris.sdk.thrift.TScanCloseResult;
import org.apache.doris.sdk.thrift.TScanNextBatchParams;
import org.apache.doris.sdk.thrift.TScanOpenParams;
import org.apache.doris.sdk.thrift.TScanOpenResult;
import org.apache.doris.sdk.thrift.TStatusCode;
import org.apache.doris.spark.client.entity.Backend;
import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.config.DorisOptions;
import org.apache.doris.spark.exception.ConnectedFailedException;
import org.apache.doris.spark.exception.DorisException;
import org.apache.doris.spark.exception.DorisInternalException;
import org.apache.doris.spark.exception.OptionRequiredException;
import org.apache.doris.spark.util.ErrorMessages;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client to request Doris BE
 */
public class DorisBackendThriftClient {

    private final static Logger logger = LoggerFactory.getLogger(DorisBackendThriftClient.class);

    private final Backend backend;
    private final DorisConfig config;

    private TDorisExternalService.Client client;
    private TTransport transport;

    private boolean isConnected = false;
    private final int retries;

    public DorisBackendThriftClient(Backend backend, DorisConfig config) throws ConnectedFailedException, OptionRequiredException {
        this.backend = backend;
        this.config = config;
        this.retries = config.getValue(DorisOptions.DORIS_REQUEST_RETRIES);
        open();
    }

    private void open() throws ConnectedFailedException {
        logger.debug("Open client to Doris BE '{}'.", backend);
        Exception ex = null;
        for (int attempt = 0; !isConnected && attempt < retries; ++attempt) {
            logger.debug("Attempt {} to connect {}.", attempt, backend);
            try {
                TBinaryProtocol.Factory factory = new TBinaryProtocol.Factory();
                TConfiguration tConf = new TConfiguration();
                Integer maxMessageSize = config.getValue(DorisOptions.DORIS_THRIFT_MAX_MESSAGE_SIZE);
                tConf.setMaxMessageSize(maxMessageSize);
                Integer connectTimeout = config.getValue(DorisOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS);
                Integer socketTimeout = config.getValue(DorisOptions.DORIS_REQUEST_READ_TIMEOUT_MS);
                logger.trace("connect timeout set to '{}'. socket timeout set to '{}'. retries set to '{}'.",
                        connectTimeout, socketTimeout, this.retries);
                transport = new TSocket(tConf, backend.getHost(), backend.getRpcPort(), socketTimeout, connectTimeout);
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
     * Open a scanner for reading Doris data.
     * @param openParams thrift struct to required by request
     * @return scan open result
     * @throws ConnectedFailedException throw if cannot connect to Doris BE
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
     * get next row batch from Doris BE
     * @param nextBatchParams thrift struct to required by request
     * @return scan batch result
     * @throws ConnectedFailedException throw if cannot connect to Doris BE
     */
    public TScanBatchResult getNext(TScanNextBatchParams nextBatchParams) throws DorisException {
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
            logger.error(ErrorMessages.DORIS_INTERNAL_FAIL_MESSAGE, backend, result.getStatus().getStatusCode(),
                    result.getStatus().getErrorMsgs());
            throw new DorisInternalException(backend.toString(), result.getStatus().getStatusCode(),
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
                logger.warn("Cannot connect to Doris BE {} when close scanner.", backend);
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
        logger.info("CloseScanner to Doris BE '{}' success.", backend);
        close();
    }
}
