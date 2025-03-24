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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.doris.spark.client.DorisBackendHttpClient;
import org.apache.doris.spark.client.DorisFrontendClient;
import org.apache.doris.spark.client.entity.Backend;
import org.apache.doris.spark.client.entity.StreamLoadResponse;
import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.config.DorisOptions;
import org.apache.doris.spark.exception.OptionRequiredException;
import org.apache.doris.spark.exception.StreamLoadException;
import org.apache.doris.spark.load.DataFormat;
import org.apache.doris.spark.util.HttpUtils;
import org.apache.doris.spark.util.URLs;
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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public abstract class AbstractStreamLoadProcessor<R> implements DorisWriter<R>, DorisCommitter {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass().getName().replaceAll("\\$", ""));

    protected static final JsonMapper MAPPER = JsonMapper.builder().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false).build();

    private static final String PARTIAL_COLUMNS = "partial_columns";
    private static final String GROUP_COMMIT = "group_commit";
    private static final Set<String> VALID_GROUP_MODE = new HashSet<>(Arrays.asList("sync_mode", "async_mode", "off_mode"));

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

    protected String columnSeparator;

    private byte[] lineDelimiter;

    private final boolean isGzipCompressionEnabled;

    private String groupCommit;

    private final boolean isPassThrough;

    private PipedOutputStream output;

    private boolean createNewBatch = true;

    private boolean isFirstRecordOfBatch = true;

    private final List<R> recordBuffer = new LinkedList<>();

    private static final int arrowBufferSize = 1000;

    private transient ExecutorService executor;

    private Future<CloseableHttpResponse> requestFuture = null;

    public AbstractStreamLoadProcessor(DorisConfig config) throws Exception {
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
        this.isGzipCompressionEnabled = properties.containsKey("compress_type") && "gzip".equals(properties.get("compress_type"));
        if (properties.containsKey(GROUP_COMMIT)) {
            String message = "";
            if (!isTwoPhaseCommitEnabled) message = "group commit does not support two-phase commit";
            if (properties.containsKey(PARTIAL_COLUMNS) && "true".equalsIgnoreCase(properties.get(PARTIAL_COLUMNS)))
                message = "group commit does not support partial column updates";
            if (!VALID_GROUP_MODE.contains(properties.get(GROUP_COMMIT).toLowerCase()))
                message = "Unsupported group commit mode: " + properties.get(GROUP_COMMIT);
            if (!message.isEmpty()) throw new IllegalArgumentException(message);
            groupCommit = properties.get(GROUP_COMMIT).toLowerCase();
        }
        this.isPassThrough = config.getValue(DorisOptions.DORIS_SINK_STREAMING_PASSTHROUGH);
    }

    public void load(R row) throws Exception {
        if (createNewBatch) {
            if (autoRedirect) {
                requestFuture = frontend.requestFrontends((frontEnd, httpClient) -> {
                    if (isTwoPhaseCommitEnabled && frontEnd.getHttpPort() <= 0) {
                        throw new IllegalArgumentException("option [" + DorisOptions.DORIS_FENODES.getName()
                                + "] is not in correct format when ["
                                + DorisOptions.DORIS_SINK_ENABLE_2PC.getName() + " = true"
                                + "], for example: host:port[,host2:port]");
                    }
                    return buildReqAndExec(frontEnd.getHost(), frontEnd.getHttpPort(), httpClient);
                });
            } else {
                requestFuture = backendHttpClient.executeReq((backend, httpClient) ->
                        buildReqAndExec(backend.getHost(), backend.getHttpPort(), httpClient));
            }
            createNewBatch = false;
        }
        if (isFirstRecordOfBatch) {
            isFirstRecordOfBatch = false;
        } else {
            output.write(lineDelimiter);
        }
        output.write(toFormat(row, format));
    }

    @Override
    public String stop() throws Exception {
        if (requestFuture != null) {
            createNewBatch = true;
            isFirstRecordOfBatch = true;
            // arrow format need to send all buffer data before stop
            if (!recordBuffer.isEmpty() && DataFormat.ARROW == format) {
                List<R> rs = new LinkedList<>(recordBuffer);
                recordBuffer.clear();
                output.write(toArrowFormat(rs));
            }
            output.close();
            CloseableHttpResponse res = requestFuture.get();
            if (res.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new StreamLoadException("stream load execute failed, status: " + res.getStatusLine().getStatusCode()
                    + ", msg: " + res.getStatusLine().getReasonPhrase());
            }
            String resEntity = EntityUtils.toString(new BufferedHttpEntity(res.getEntity()));
            logger.info("stream load response: {}", resEntity);
            StreamLoadResponse response = MAPPER.readValue(resEntity, StreamLoadResponse.class);
            if (response != null && response.isSuccess()) {
                return isTwoPhaseCommitEnabled ? String.valueOf(response.getTxnId()) : null;
            } else {
                throw new StreamLoadException("stream load execute failed, response: " + resEntity);
            }
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
                this.logger.info("commit: {} response: {}", msg, resEntity);
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
                this.logger.info("abort: {} response: {}", msg, resEntity);
            }
        } catch (IOException e) {
            throw new RuntimeException("abort transaction failed, transaction: " + msg, e);
        }
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
        return isPassThrough ? getPassThroughData(row) : stringify(row, format);
    }

    protected abstract byte[] getPassThroughData(R row);

    public abstract byte[] stringify(R row, DataFormat format);

    public abstract byte[] toArrowFormat(List<R> rows) throws IOException;

    public abstract String getWriteFields() throws OptionRequiredException;

    private void handleStreamLoadProperties(HttpPut httpPut) throws OptionRequiredException {
        addCommonHeaders(httpPut);
        if (groupCommit == null || groupCommit.equals("off_mode")) {
            String label = generateStreamLoadLabel();
            httpPut.setHeader("label", label);
        }
        String writeFields = getWriteFields();
        httpPut.setHeader("columns", writeFields);
        if (config.contains(DorisOptions.DORIS_MAX_FILTER_RATIO)) {
            httpPut.setHeader("max_filter_ratio", config.getValue(DorisOptions.DORIS_MAX_FILTER_RATIO));
        }
        if (isTwoPhaseCommitEnabled) httpPut.setHeader("two_phase_commit", "true");

        switch (format) {
            case CSV:
                if (!properties.containsKey("column_separator")) {
                    properties.put("column_separator", "\t");
                }
                columnSeparator = properties.get("column_separator");
                if (!properties.containsKey("line_delimiter")) {
                    properties.put("line_delimiter", "\n");
                }
                lineDelimiter = properties.get("line_delimiter").getBytes(StandardCharsets.UTF_8);
                break;
            case JSON:
                if (!properties.containsKey("line_delimiter")) {
                    properties.put("line_delimiter", "\n");
                }
                lineDelimiter = properties.get("line_delimiter").getBytes(StandardCharsets.UTF_8);
                properties.put("read_json_by_line", "true");
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

    private Future<CloseableHttpResponse> buildReqAndExec(String host, Integer port, CloseableHttpClient client) {
        HttpPut httpPut = new HttpPut(URLs.streamLoad(host, port, database, table, isHttpsEnabled));
        try {
            handleStreamLoadProperties(httpPut);
        } catch (OptionRequiredException e) {
            throw new RuntimeException("stream load handle properties failed", e);
        }
        PipedInputStream pipedInputStream = new PipedInputStream(4096);
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
        return getExecutors().submit(() -> client.execute(httpPut));
    }

    @Override
    public void close() throws IOException {
        createNewBatch = true;
        isFirstRecordOfBatch = true;
        frontend.close();
        backendHttpClient.close();
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

}
