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
import org.apache.commons.lang3.ArrayUtils;
import org.apache.doris.spark.client.DorisBackendHttpClient;
import org.apache.doris.spark.client.DorisFrontendClient;
import org.apache.doris.spark.client.entity.Backend;
import org.apache.doris.spark.client.entity.StreamLoadResponse;
import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.config.DorisOptions;
import org.apache.doris.spark.exception.OptionRequiredException;
import org.apache.doris.spark.exception.StreamLoadException;
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

    private static final String[] STREAM_LOAD_SUCCESS_STATUS = {"Success", "Publish Timeout"};

    private final boolean autoRedirect;

    private final boolean isHttpsEnabled;

    private final boolean isTwoPhaseCommitEnabled;

    private final Map<String, String> properties;

    private final String format;

    protected String columnSeparator;

    private String lineDelimiter;

    private final boolean isGzipCompressionEnabled;

    private String groupCommit;

    private final boolean isPassThrough;

    private PipedOutputStream output;

    private boolean createNewBatch = true;

    private boolean isFirstRecordOfBatch = true;

    private final List<R> recordBuffer = new LinkedList<>();

    private static final int arrowBufferSize = 1000;

    private final static ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable);
        thread.setName("stream-load-worker-" + new AtomicInteger().getAndIncrement());
        thread.setDaemon(true);
        return thread;
    });

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
        this.format = properties.getOrDefault("format", "csv");
        this.isGzipCompressionEnabled = properties.containsKey("compress_type") && "gzip".equals(properties.get("compress_type"));
        if (properties.containsKey(GROUP_COMMIT)) {
            String message = "";
            if (!isTwoPhaseCommitEnabled) message = "1";// todo
            if (properties.containsKey(PARTIAL_COLUMNS) && "true".equalsIgnoreCase(properties.get(PARTIAL_COLUMNS))) message = "2";// todo
            if (!VALID_GROUP_MODE.contains(properties.get(GROUP_COMMIT).toLowerCase())) message = "3";// todo
            if (!message.isEmpty()) throw new IllegalArgumentException(message);
            groupCommit = properties.get(GROUP_COMMIT).toLowerCase();
        }
        this.isPassThrough = config.getValue(DorisOptions.DORIS_SINK_STREAMING_PASSTHROUGH);
    }

    public void load(R row) throws Exception {
        if (createNewBatch) {
            if (autoRedirect) {
                requestFuture = frontend.requestFrontends((frontEnd, httpClient) ->
                        buildReqAndExec(frontEnd.getHost(), frontEnd.getHttpPort(), httpClient));
            } else {
                requestFuture = backendHttpClient.executeReq((backend, httpClient) ->
                        buildReqAndExec(backend.getHost(), backend.getHttpPort(), httpClient));
            }
            createNewBatch = false;
        }
        output.write(toFormat(row, format));
    }

    @Override
    public String stop() throws Exception {
        // arrow format need to send all buffer data before stop
        if (!recordBuffer.isEmpty() && "arrow".equalsIgnoreCase(format)) {
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
        StreamLoadResponse response = MAPPER.readValue(resEntity, StreamLoadResponse.class);
        if (ArrayUtils.contains(STREAM_LOAD_SUCCESS_STATUS, response.getStatus())) {
            createNewBatch = true;
            return isTwoPhaseCommitEnabled ? String.valueOf(response.getTxnId()) : null;
        } else {
            throw new StreamLoadException("stream load execute failed, status: " + response.getStatus()
                    + ", msg: " + response.getMessage() + ", errUrl: " + response.getErrorURL());
        }
    }

    @Override
    public void commit(String msg) throws Exception {
        if (isTwoPhaseCommitEnabled) {
            logger.info("begin to commit transaction {}", msg);
            frontend.requestFrontends((frontEnd, httpClient) -> {
                HttpPut httpPut = new HttpPut(URLs.streamLoad2PC(frontEnd.getHost(), frontEnd.getHttpPort(), database, isHttpsEnabled));
                try {
                    handleCommitHeaders(httpPut, msg);
                } catch (OptionRequiredException e) {
                    throw new RuntimeException("stream load handle commit props failed", e);
                }
                try {
                    CloseableHttpResponse response = httpClient.execute(httpPut);
                    if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                        throw new RuntimeException("commit transaction failed, transaction: " + msg
                                + ", status: " + response.getStatusLine().getStatusCode()
                                + ", reason: " + response.getStatusLine().getReasonPhrase());
                    }
                } catch (IOException e) {
                    throw new RuntimeException("commit transaction failed, transaction: " + msg, e);
                }
                return null;
            });
            logger.info("success to commit transaction {}", msg);
        }
    }

    @Override
    public void abort(String msg) throws Exception {
        if (isTwoPhaseCommitEnabled) {
            logger.info("begin to abort transaction {}", msg);
            frontend.requestFrontends((frontEnd, httpClient) -> {
                HttpPut httpPut = new HttpPut(URLs.streamLoad2PC(frontEnd.getHost(), frontEnd.getHttpPort(), database, isHttpsEnabled));
                try {
                    handleAbortHeaders(httpPut, msg);
                } catch (OptionRequiredException e) {
                    throw new RuntimeException("stream load handle abort props failed", e);
                }
                try {
                    CloseableHttpResponse response = httpClient.execute(httpPut);
                    if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                        throw new RuntimeException("abort transaction failed, transaction: " + msg
                                + ", status: " + response.getStatusLine().getStatusCode()
                                + ", reason: " + response.getStatusLine().getReasonPhrase());
                    }
                } catch (IOException e) {
                    throw new RuntimeException("abort transaction failed, transaction: " + msg, e);
                }
                return null; // Returning null as the callback does not return anything
            });
            logger.info("success to abort transaction {}", msg);
        }
    }

    private byte[] toFormat(R row, String format) throws IOException {
        switch (format.toLowerCase()) {
            case "csv":
            case "json":
                return toStringFormat(row, format);
            case "arrow":
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

    private byte[] toStringFormat(R row, String format) {
        String prefix = isFirstRecordOfBatch ? "" : lineDelimiter;
        isFirstRecordOfBatch = false;
        String stringRow = isPassThrough ? getPassThroughData(row) : stringify(row, format);
        return (prefix + stringRow).getBytes(StandardCharsets.UTF_8);
    }

    protected abstract String getPassThroughData(R row);

    public abstract String stringify(R row, String format);

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

        switch (format.toLowerCase()) {
            case "csv":
                if (!properties.containsKey("column_separator")) {
                    properties.put("column_separator", "\t");
                }
                columnSeparator = properties.get("column_separator");
                if (!properties.containsKey("line_delimiter")) {
                    properties.put("line_delimiter", "\n");
                }
                lineDelimiter = properties.get("line_delimiter");
                break;
            case "json":
                if (!properties.containsKey("line_delimiter")) {
                    properties.put("line_delimiter", "\n");
                }
                lineDelimiter = properties.get("line_delimiter");
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
        return executor.submit(() -> client.execute(httpPut));
    }

    @Override
    public void close() throws IOException {
        createNewBatch = true;
        frontend.close();
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

}
