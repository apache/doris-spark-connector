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

package org.apache.doris.spark.client.write;

import org.apache.doris.spark.client.DorisBackendHttpClient;
import org.apache.doris.spark.client.DorisFrontendClient;
import org.apache.doris.spark.client.entity.Backend;
import org.apache.doris.spark.client.entity.StreamLoadResponse;
import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.config.DorisOptions;
import org.apache.doris.spark.exception.OptionRequiredException;
import org.apache.doris.spark.exception.StreamLoadException;
import org.apache.doris.spark.rest.models.DataFormat;
import org.apache.doris.spark.util.EscapeHandler;
import org.apache.doris.spark.util.HttpUtils;
import org.apache.doris.spark.util.URLs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.entity.GzipCompressingEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.doris.spark.config.DorisOptions.GROUP_COMMIT;
import static org.apache.doris.spark.config.DorisOptions.PARTIAL_COLUMNS;
import static org.apache.doris.spark.config.DorisOptions.VALID_GROUP_MODE;

public abstract class AbstractStreamLoadProcessor<R> extends DorisWriter<R> implements DorisCommitter {

    protected static final JsonMapper MAPPER =
            JsonMapper.builder().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false).build();
    private static final int arrowBufferSize = 1000;
    protected final Logger logger = LoggerFactory.getLogger(this.getClass().getName().replaceAll("\\$", ""));
    protected final DorisConfig config;
    private final DorisFrontendClient frontend;
    private final DorisBackendHttpClient backendHttpClient;
    private final String database;
    private final String table;
    private final boolean autoRedirect;
    private final boolean isHttpsEnabled;
    private final boolean isTwoPhaseCommitEnabled;
    private final Map<String, String> properties;
    private final DataFormat format;
    private final boolean isGzipCompressionEnabled;
    private final boolean isPassThrough;
    private final List<R> recordBuffer = new LinkedList<>();
    private final int pipeSize;
    protected String columnSeparator;
    private byte[] lineDelimiter;
    private String groupCommit;
    private PipedOutputStream output;
    private transient ExecutorService executor;

    private Future<StreamLoadResponse> requestFuture = null;
    private volatile String currentLabel;
    private Exception unexpectedException = null;

    public AbstractStreamLoadProcessor(DorisConfig config) throws Exception {
        super(config.getValue(DorisOptions.DORIS_SINK_BATCH_SIZE));
        this.config = config;
        String tableIdentifier = config.getValue(DorisOptions.DORIS_TABLE_IDENTIFIER);
        String[] dbTableArr = tableIdentifier.split("\\.");
        this.database = dbTableArr[0].replaceAll("`", "").trim();
        this.table = dbTableArr[1].replaceAll("`", "").trim();
        this.frontend = new DorisFrontendClient(config);
        this.autoRedirect = config.getValue(DorisOptions.DORIS_SINK_AUTO_REDIRECT);
        this.backendHttpClient = autoRedirect ? null : new DorisBackendHttpClient(getBackends());
        this.isHttpsEnabled = config.getValue(DorisOptions.DORIS_ENABLE_HTTPS);
        this.properties = config.getSinkProperties();
        // init stream load props
        this.isTwoPhaseCommitEnabled = config.getValue(DorisOptions.DORIS_SINK_ENABLE_2PC);
        this.format = DataFormat.valueOf(properties.getOrDefault("format", "csv").toUpperCase());
        this.isGzipCompressionEnabled =
                properties.containsKey("compress_type") && "gzip".equals(properties.get("compress_type"));
        if (properties.containsKey(GROUP_COMMIT)) {
            String message = "";
            if (isTwoPhaseCommitEnabled) {
                message = "group commit does not support two-phase commit";
            }
            if (properties.containsKey(PARTIAL_COLUMNS) && "true".equalsIgnoreCase(properties.get(PARTIAL_COLUMNS))) {
                message = "group commit does not support partial column updates";
            }
            if (!VALID_GROUP_MODE.contains(properties.get(GROUP_COMMIT).toLowerCase())) {
                message = "Unsupported group commit mode: " + properties.get(GROUP_COMMIT);
            }
            if (!message.isEmpty()) {
                throw new IllegalArgumentException(message);
            }
            groupCommit = properties.get(GROUP_COMMIT).toLowerCase();
        }
        this.isPassThrough = config.getValue(DorisOptions.DORIS_SINK_STREAMING_PASSTHROUGH);
        this.pipeSize = config.getValue(DorisOptions.DORIS_SINK_NET_BUFFER_SIZE);
    }

    public void load(R row) throws Exception {
        if (createNewBatch) {
            createNewBatch = false;
            if (autoRedirect) {
                requestFuture = frontend.requestFrontends((frontEnd, httpClient) ->
                        buildReqAndExec(frontEnd.getHost(), frontEnd.getHttpPort(), httpClient));
            } else {
                requestFuture = backendHttpClient.executeReq((backend, httpClient) ->
                        buildReqAndExec(backend.getHost(), backend.getHttpPort(), httpClient));
            }
        }
        if (isFirstRecordOfBatch) {
            isFirstRecordOfBatch = false;
        } else if (lineDelimiter != null) {
            writeTo(lineDelimiter);
        }
        writeTo(toFormat(row, format));
        currentBatchCount++;
    }

    @Override
    public String stop() throws Exception {
        if (requestFuture != null) {
            createNewBatch = true;
            isFirstRecordOfBatch = true;
            // arrow format need to send all buffer data before stop
            if (!recordBuffer.isEmpty() && DataFormat.ARROW.equals(format)) {
                List<R> rs = new LinkedList<>(recordBuffer);
                recordBuffer.clear();
                writeTo(toArrowFormat(rs));
            }
            output.close();
            logger.info("stream load stopped with {}", currentLabel != null ? currentLabel : "group commit");

            StreamLoadResponse response;
            try {
                response = requestFuture.get();
                if (response == null) {
                    throw new StreamLoadException("response is null");
                }
            } catch (Exception e) {
                if (unexpectedException != null) {
                    throw unexpectedException;
                }
                throw new StreamLoadException("stream load stop failed", e);
            }
            return isTwoPhaseCommitEnabled ? String.valueOf(response.getTxnId()) : null;
        }
        return null;
    }

    private void execCommitReq(String host, int port, String msg, CloseableHttpClient httpClient) {
        HttpPut httpPut = new HttpPut(URLs.streamLoad2PC(host, port, database, isHttpsEnabled));
        try {
            handleCommitHeaders(httpPut, msg);
        } catch (OptionRequiredException e) {
            throw new RuntimeException("stream load handle commit props failed", e);
        }
        try {
            CloseableHttpResponse response = httpClient.execute(httpPut);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new RuntimeException("commit transaction failed, transaction: " + msg + ", status: "
                        + response.getStatusLine().getStatusCode() + ", reason: " + response.getStatusLine()
                        .getReasonPhrase());
            } else {
                String resEntity = EntityUtils.toString(new BufferedHttpEntity(response.getEntity()));
                if (!checkTransResponse(resEntity)) {
                    throw new RuntimeException(
                            "commit transaction failed, transaction: " + msg + ", resp: " + resEntity);
                } else {
                    this.logger.info("commit: {} response: {}", msg, resEntity);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("commit transaction failed, transaction: " + msg, e);
        }
    }

    @Override
    public void commit(String msg) throws Exception {
        if (isTwoPhaseCommitEnabled) {
            logger.info("begin to commit transaction {}", msg);
            if (autoRedirect) {
                frontend.requestFrontends((frontEnd, httpClient) -> {
                    execCommitReq(frontEnd.getHost(), frontEnd.getHttpPort(), msg, httpClient);
                    return null;
                });
            } else {
                backendHttpClient.executeReq((backend, httpClient) -> {
                    execCommitReq(backend.getHost(), backend.getHttpPort(), msg, httpClient);
                    return null;
                });
            }
            logger.info("success to commit transaction {}", msg);
        }
    }

    private void execAbortReq(String host, int port, String msg, CloseableHttpClient httpClient) {
        HttpPut httpPut = new HttpPut(URLs.streamLoad2PC(host, port, database, isHttpsEnabled));
        try {
            handleAbortHeaders(httpPut, msg);
        } catch (OptionRequiredException e) {
            throw new RuntimeException("stream load handle abort props failed", e);
        }
        try {
            CloseableHttpResponse response = httpClient.execute(httpPut);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new RuntimeException("abort transaction failed, transaction: " + msg + ", status: "
                        + response.getStatusLine().getStatusCode() + ", reason: " + response.getStatusLine()
                        .getReasonPhrase());
            } else {
                String resEntity = EntityUtils.toString(new BufferedHttpEntity(response.getEntity()));
                if (!checkTransResponse(resEntity)) {
                    throw new RuntimeException(
                            "abort transaction failed, transaction: " + msg + ", resp: " + resEntity);
                } else {
                    this.logger.info("abort: {} response: {}", msg, resEntity);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("abort transaction failed, transaction: " + msg, e);
        }
    }

    private boolean checkTransResponse(String resEntity) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String status = objectMapper.readTree(resEntity).get("status").asText();
            if ("Success".equalsIgnoreCase(status)) {
                return true;
            }
        } catch (JsonProcessingException e) {
            logger.warn("invalid json response: " + resEntity, e);
        }
        return false;
    }

    @Override
    public void abort(String msg) throws Exception {
        if (isTwoPhaseCommitEnabled) {
            logger.info("begin to abort transaction {}", msg);
            if (autoRedirect) {
                frontend.requestFrontends((frontEnd, httpClient) -> {
                    execAbortReq(frontEnd.getHost(), frontEnd.getHttpPort(), msg, httpClient);
                    return null; // Returning null as the callback does not return anything
                });
            } else {
                backendHttpClient.executeReq((backend, httpClient) -> {
                    execAbortReq(backend.getHost(), backend.getHttpPort(), msg, httpClient);
                    return null; // Returning null as the callback does not return anything
                });
            }
            logger.info("success to abort transaction {}", msg);
        }
    }

    private byte[] toFormat(R row, DataFormat format) throws IOException {
        switch (format) {
            case CSV:
            case JSON:
                return toStringFormat(row, format);
            case ARROW:
                recordBuffer.add(copy(row));
                if (recordBuffer.size() < arrowBufferSize) {
                    return new byte[0];
                } else {
                    LinkedList<R> rs = new LinkedList<>(recordBuffer);
                    recordBuffer.clear();
                    return toArrowFormat(rs);
                }
            default:
                throw new IllegalArgumentException("Unsupported stream load format: " + format);
        }
    }

    private byte[] toStringFormat(R row, DataFormat format) {
        return isPassThrough ? getPassThroughData(row).getBytes(StandardCharsets.UTF_8) : stringify(row, format);
    }

    protected abstract String getPassThroughData(R row);

    public abstract byte[] stringify(R row, DataFormat format);

    public abstract byte[] toArrowFormat(List<R> rows) throws IOException;

    public abstract String getWriteFields() throws OptionRequiredException;

    private void handleStreamLoadProperties(HttpPut httpPut) throws OptionRequiredException {
        addCommonHeaders(httpPut);
        if (groupCommit == null || groupCommit.equals("off_mode")) {
            currentLabel = generateStreamLoadLabel();
            httpPut.setHeader("label", currentLabel);
        }
        String writeFields = getWriteFields();
        httpPut.setHeader("columns", writeFields);
        if (config.contains(DorisOptions.DORIS_MAX_FILTER_RATIO)) {
            httpPut.setHeader("max_filter_ratio", config.getValue(DorisOptions.DORIS_MAX_FILTER_RATIO));
        }
        if (isTwoPhaseCommitEnabled) {
            httpPut.setHeader("two_phase_commit", "true");
        }

        switch (format) {
            case CSV:
                // Handling hidden delimiters
                columnSeparator = EscapeHandler.escapeString(properties.getOrDefault("column_separator", "\t"));
                lineDelimiter = EscapeHandler.escapeString(properties.getOrDefault("line_delimiter", "\n")
                ).getBytes(StandardCharsets.UTF_8);
                break;
            case JSON:
                lineDelimiter = properties.getOrDefault("line_delimiter", "\n").getBytes(
                        StandardCharsets.UTF_8);
                properties.put("read_json_by_line", "true");
                break;
            case ARROW:
                lineDelimiter = null;
                break;
        }
        properties.forEach(httpPut::setHeader);
    }

    private void handleCommitHeaders(HttpPut httpPut, String transactionId) throws OptionRequiredException {
        addCommonHeaders(httpPut);
        httpPut.setHeader("txn_operation", "commit");
        httpPut.setHeader("txn_id", transactionId);
    }

    private void handleAbortHeaders(HttpPut httpPut, String transactionId) throws OptionRequiredException {
        addCommonHeaders(httpPut);
        httpPut.setHeader("txn_operation", "abort");
        httpPut.setHeader("txn_id", transactionId);
    }

    private void addCommonHeaders(HttpRequestBase req) throws OptionRequiredException {
        String user = config.getValue(DorisOptions.DORIS_USER);
        String password = config.getValue(DorisOptions.DORIS_PASSWORD);
        HttpUtils.setAuth(req, user, password);
        req.setHeader(HttpHeaders.EXPECT, "100-continue");
        req.setHeader(HttpHeaders.CONTENT_TYPE, "text/plain; charset=UTF-8");
    }

    protected abstract String generateStreamLoadLabel() throws OptionRequiredException;

    private Future<StreamLoadResponse> buildReqAndExec(String host, Integer port, CloseableHttpClient client) {
        HttpPut httpPut = new HttpPut(URLs.streamLoad(host, port, database, table, isHttpsEnabled));
        try {
            handleStreamLoadProperties(httpPut);
        } catch (OptionRequiredException e) {
            throw new RuntimeException("stream load handle properties failed", e);
        }
        PipedInputStream pipedInputStream = new PipedInputStream(pipeSize);
        try {
            output = new PipedOutputStream(pipedInputStream);
        } catch (IOException e) {
            throw new RuntimeException("stream load create output failed", e);
        }
        HttpEntity entity = new InputStreamEntity(pipedInputStream);
        if (isGzipCompressionEnabled) {
            entity = new GzipCompressingEntity(entity);
        }
        httpPut.setEntity(entity);
        Thread currentThread = Thread.currentThread();

        logger.info("table {}.{} stream load started for {} on host {}:{}", database, table,
                currentLabel != null ? currentLabel : "group commit", host, port);
        return getExecutors().submit(() -> {
            StreamLoadResponse streamLoadResponse = null;
            try (CloseableHttpResponse response = client.execute(httpPut)) {
                // stream load http request finished unexpectedly
                if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                    throw new StreamLoadException(
                            "stream load failed, status: " + response.getStatusLine().getStatusCode()
                                    + ", reason: " + response.getStatusLine().getReasonPhrase());
                }
                String entityStr = EntityUtils.toString(response.getEntity());
                logger.info("stream load response: " + entityStr);
                streamLoadResponse = MAPPER.readValue(entityStr, StreamLoadResponse.class);
                if (streamLoadResponse == null
                        || streamLoadResponse.getLabel() == null
                        || streamLoadResponse.getMessage() == null) {
                    throw new StreamLoadException("stream load failed, response error : " + entityStr);
                } else if (!streamLoadResponse.isSuccess()) {
                    throw new StreamLoadException(
                            "stream load failed, txnId: " + streamLoadResponse.getTxnId()
                                    + ", status: " + streamLoadResponse.getStatus()
                                    + ", msg: " + streamLoadResponse.getMessage());
                }
            } catch (Exception e) {
                logger.error("stream load exception", e);
                unexpectedException = e;
                currentThread.interrupt();
                throw e;
            }
            return streamLoadResponse;
        });
    }

    @Override
    public void close() throws IOException {
        createNewBatch = true;
        isFirstRecordOfBatch = true;
        unexpectedException = null;
        frontend.close();
        if (backendHttpClient != null) {
            backendHttpClient.close();
        }
        if (executor != null && !executor.isShutdown()) {
            executor.shutdown();
        }
    }

    private List<Backend> getBackends() throws Exception {
        if (config.contains(DorisOptions.DORIS_BENODES)) {
            String beNodes = config.getValue(DorisOptions.DORIS_BENODES);
            String[] beNodesArr = beNodes.split("\\.");
            return Arrays.stream(beNodesArr).map(beNode -> {
                String[] beNodeArr = beNode.split(":");
                return new Backend(beNodeArr[0], Integer.valueOf(beNodeArr[1]), -1);
            }).collect(Collectors.toList());
        } else {
            return frontend.getAliveBackends();
        }
    }

    protected abstract R copy(R row);

    private synchronized ExecutorService getExecutors() {
        if (executor == null) {
            executor = Executors.newSingleThreadExecutor(runnable -> {
                Thread thread = new Thread(runnable);
                thread.setName("stream-load-worker-" + new AtomicInteger().getAndIncrement());
                thread.setDaemon(true);
                return thread;
            });
        }
        return executor;
    }

    private void writeTo(byte[] bytes) throws Exception {
        try {
            output.write(bytes);
        } catch (Exception e) {
            if (unexpectedException != null) {
                throw unexpectedException;
            } else {
                throw e;
            }
        }
    }

}
