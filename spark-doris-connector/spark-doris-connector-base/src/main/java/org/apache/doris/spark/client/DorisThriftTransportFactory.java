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

import org.apache.doris.spark.client.entity.Backend;
import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.config.DorisOptions;
import org.apache.doris.spark.config.DorisTlsOptions;
import org.apache.doris.spark.exception.OptionRequiredException;
import org.apache.doris.spark.util.DorisTlsContextFactory;

import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/** Creates plain or TLS Thrift transports according to the connector TLS policy. */
final class DorisThriftTransportFactory {

    private DorisThriftTransportFactory() {
    }

    static TTransport create(Backend backend, DorisConfig config)
            throws OptionRequiredException, TTransportException {
        TConfiguration thriftConfig = new TConfiguration();
        thriftConfig.setMaxMessageSize(
                config.getValue(DorisOptions.DORIS_THRIFT_MAX_MESSAGE_SIZE));
        int connectTimeout =
                config.getValue(DorisOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS);
        int socketTimeout = config.getValue(DorisOptions.DORIS_REQUEST_READ_TIMEOUT_MS);
        DorisTlsOptions tlsOptions = config.getTlsOptions();
        if (!tlsOptions.isEnabledFor(DorisTlsOptions.Protocol.THRIFT)) {
            return new TSocket(
                    thriftConfig,
                    backend.getHost(),
                    backend.getRpcPort(),
                    socketTimeout,
                    connectTimeout);
        }

        return new TlsTransport(
                thriftConfig,
                backend.getHost(),
                backend.getRpcPort(),
                socketTimeout,
                connectTimeout,
                DorisTlsContextFactory.createSslContext(tlsOptions),
                tlsOptions.isSkipHostnameVerification());
    }

    private static final class TlsTransport extends TIOStreamTransport {

        private final String host;
        private final int port;
        private final int socketTimeout;
        private final int connectTimeout;
        private final SSLContext sslContext;
        private final boolean skipHostnameVerification;
        private SSLSocket socket;

        private TlsTransport(
                TConfiguration thriftConfig,
                String host,
                int port,
                int socketTimeout,
                int connectTimeout,
                SSLContext sslContext,
                boolean skipHostnameVerification)
                throws TTransportException {
            super(thriftConfig);
            this.host = host;
            this.port = port;
            this.socketTimeout = socketTimeout;
            this.connectTimeout = connectTimeout;
            this.sslContext = sslContext;
            this.skipHostnameVerification = skipHostnameVerification;
        }

        @Override
        public boolean isOpen() {
            return socket != null && socket.isConnected() && !socket.isClosed();
        }

        @Override
        public void open() throws TTransportException {
            if (isOpen()) {
                throw new TTransportException(TTransportException.ALREADY_OPEN);
            }

            Socket plainSocket = new Socket();
            try {
                plainSocket.connect(new InetSocketAddress(host, port), connectTimeout);
                plainSocket.setSoTimeout(socketTimeout);
                socket = (SSLSocket) sslContext.getSocketFactory()
                        .createSocket(plainSocket, host, port, true);
                SSLParameters parameters = socket.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm(
                        skipHostnameVerification ? null : "HTTPS");
                socket.setSSLParameters(parameters);
                socket.startHandshake();
                inputStream_ = new BufferedInputStream(socket.getInputStream());
                outputStream_ = new BufferedOutputStream(socket.getOutputStream());
            } catch (IOException e) {
                closeSocket(plainSocket);
                close();
                throw new TTransportException(TTransportException.NOT_OPEN, e);
            }
        }

        @Override
        public void close() {
            super.close();
            if (socket != null) {
                closeSocket(socket);
                socket = null;
            }
        }

        private static void closeSocket(Socket socket) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // Nothing else can be done while closing the transport.
            }
        }
    }
}
