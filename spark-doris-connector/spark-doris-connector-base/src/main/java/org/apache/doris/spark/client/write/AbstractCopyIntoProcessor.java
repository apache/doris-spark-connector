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

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.doris.spark.client.DorisFrontendClient;
import org.apache.doris.spark.client.entity.Frontend;
import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.config.DorisOptions;
import org.apache.doris.spark.exception.CopyIntoException;
import org.apache.doris.spark.exception.OptionRequiredException;
import org.apache.doris.spark.load.CopyIntoResponse;
import org.apache.doris.spark.rest.models.RespContent;
import org.apache.doris.spark.util.CopySQLBuilder;
import org.apache.doris.spark.util.HttpPostBuilder;
import org.apache.doris.spark.util.HttpPutBuilder;
import org.apache.doris.spark.util.URLs;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.entity.GzipCompressingEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractCopyIntoProcessor<R> extends DorisWriter<R> implements DorisCommitter {

    protected static final Logger LOG = LoggerFactory.getLogger("CopyIntoProcessor");

    protected static final JsonMapper MAPPER = JsonMapper.builder().build();

    protected final DorisConfig config;

    protected final DorisFrontendClient frontend;

    private final Map<String, String> properties;

    private final String format;

    protected String columnSeparator;

    private String lineDelimiter;

    private final boolean isGzipCompressionEnabled;

    private PipedOutputStream output;

    private String fileName;

    private final static ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable);
        thread.setName("copy-into-worker-" + new AtomicInteger().getAndIncrement());
        thread.setDaemon(true);
        return thread;
    });

    private Future<HttpResponse> requestFuture = null;

    private boolean isFirstRecordOfBatch = true;

    private boolean isNewBatch = true;

    public AbstractCopyIntoProcessor(DorisConfig config) throws Exception {
        super(config.getValue(DorisOptions.DORIS_SINK_BATCH_SIZE));
        this.config = config;
        this.frontend = new DorisFrontendClient(config);
        this.properties = config.getSinkProperties();
        this.format = properties.getOrDefault("format", "csv");
        this.isGzipCompressionEnabled = properties.containsKey("compress_type") && "gzip".equals(properties.get("compress_type"));
    }

    @Override
    public void load(R row) throws Exception {
        if (isNewBatch) {
            requestFuture = frontend.requestFrontends((frontend, httpClient) -> {
                try {
                    String uploadUrl = getUploadUrl(frontend, httpClient);
                    return uploadFile(httpClient, uploadUrl);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
        isNewBatch = false;
        String dataString = "";
        if (isFirstRecordOfBatch) {
            isFirstRecordOfBatch = false;
            dataString = lineDelimiter;
        }
        dataString += toFormat(row, format);
        output.write(dataString.getBytes(StandardCharsets.UTF_8));
    }

    private String getAuthEncoded() throws OptionRequiredException {
        String user = config.getValue(DorisOptions.DORIS_USER);
        String password = config.getValue(DorisOptions.DORIS_PASSWORD);
        return Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String stop() throws Exception {
        output.close();
        HttpResponse uploadRes = requestFuture.get();
        int statusCode = uploadRes.getStatusLine().getStatusCode();
        String statusMessage = uploadRes.getStatusLine().getReasonPhrase();
        String responseContent = EntityUtils.toString(new BufferedHttpEntity(uploadRes.getEntity()), StandardCharsets.UTF_8);
        CopyIntoResponse loadResponse = new CopyIntoResponse(statusCode, statusMessage, responseContent);
        if (loadResponse.code() != HttpStatus.SC_OK) {
            LOG.error(String.format("Upload file status is not OK, status: %d, response: %s", loadResponse.code(), loadResponse));
            throw new CopyIntoException(String.format("Upload file error, http status:%d, response:%s", loadResponse.code(), loadResponse));
        } else {
            LOG.info(String.format("Upload file success, status: %d, response: %s", loadResponse.code(), loadResponse));
        }
        isNewBatch = true;
        return fileName;
    }

    @Override
    public void close() throws IOException {
        isNewBatch = true;
        frontend.close();
    }

    private String getUploadUrl(Frontend frontend, CloseableHttpClient httpClient) throws Exception {
        String uploadUrl = URLs.copyIntoUpload(frontend.getHost(), frontend.getHttpPort(), false);
        fileName = UUID.randomUUID().toString();
        HttpPut uploadReq = new HttpPutBuilder().setUrl(uploadUrl).addCommonHeader()
                .addFileName(fileName).setEntity(new StringEntity("")).baseAuth(getAuthEncoded()).build();
        CloseableHttpResponse uploadRes = httpClient.execute(uploadReq);
        if (uploadRes.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
            throw new RuntimeException("upload file failed, status: " + uploadRes.getStatusLine().getStatusCode()
                    + ", reason: " + uploadRes.getStatusLine().getReasonPhrase());
        }
        int statusCode = uploadRes.getStatusLine().getStatusCode();
        String reasonPhrase = uploadRes.getStatusLine().getReasonPhrase();
        String content = EntityUtils.toString(new BufferedHttpEntity(uploadRes.getEntity()), StandardCharsets.UTF_8);
        CopyIntoResponse loadResponse = new CopyIntoResponse(statusCode, reasonPhrase, content);
        if (loadResponse.code() == HttpStatus.SC_TEMPORARY_REDIRECT) {
            String uploadAddress = uploadRes.getFirstHeader("location").getValue();
            LOG.info("Get upload address Response: " + loadResponse);
            LOG.info("Redirect to s3: " + uploadAddress);
            return uploadAddress;
        } else {
            LOG.error("Failed to get the redirected address, status " + loadResponse.code() + ", reason " + loadResponse.msg() + ", response " + loadResponse.content());
            throw new RuntimeException("Could not get the redirected address.");
        }

    }

    private Future<HttpResponse> uploadFile(CloseableHttpClient httpClient, String uploadUrl) throws IOException {
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
                break;
        }
        PipedInputStream pipedInputStream = new PipedInputStream(4096);
        output = new PipedOutputStream(pipedInputStream);
        HttpEntity entity = new InputStreamEntity(pipedInputStream);
        if (isGzipCompressionEnabled) {
            entity = new GzipCompressingEntity(entity);
        }
        HttpPut httpPut = new HttpPutBuilder().setUrl(uploadUrl).addCommonHeader()
                .setEntity(entity).build();
        return executor.submit(() -> httpClient.execute(httpPut));
    }

    protected abstract String toFormat(R row, String format);

    private void executeCopyInto(Frontend frontend, CloseableHttpClient httpClient, String fileName) throws OptionRequiredException, IOException, CopyIntoException {
        String tableIdentifier = config.getValue(DorisOptions.DORIS_TABLE_IDENTIFIER);
        Properties props = new Properties();
        properties.forEach(props::setProperty);
        CopySQLBuilder copySQLBuilder = new CopySQLBuilder(format, props, tableIdentifier, fileName);
        String copySql = copySQLBuilder.buildCopySQL();
        LOG.info("build copy sql is " + copySql);
        ObjectNode objectNode = MAPPER.createObjectNode();
        objectNode.put("sql", copySql);
        String queryUrl = URLs.copyIntoQuery(frontend.getHost(), frontend.getHttpPort(), false);
        HttpPost queryReq = new HttpPostBuilder().setUrl(queryUrl)
                .baseAuth(getAuthEncoded())
                .setEntity(new StringEntity(MAPPER.writeValueAsString(objectNode))).build();
        CloseableHttpResponse queryRes = httpClient.execute(queryReq);
        int statusCode = queryRes.getStatusLine().getStatusCode();
        String statusMessage = queryRes.getStatusLine().getReasonPhrase();
        String responseContent = EntityUtils.toString(new BufferedHttpEntity(queryRes.getEntity()), StandardCharsets.UTF_8);
        CopyIntoResponse loadResponse = new CopyIntoResponse(statusCode, statusMessage, responseContent);

        if (loadResponse.code() != HttpStatus.SC_OK) {
            LOG.error(String.format("Execute copy sql status is not OK, status: %d, response: %s",
                    loadResponse.code(), loadResponse));
            throw new CopyIntoException(String.format("Execute copy sql, http status:%d, response:%s",
                    loadResponse.code(), loadResponse));
        } else {
            try {
                RespContent responseContentObject = MAPPER.readValue(loadResponse.content(), RespContent.class);
                if (!responseContentObject.isCopyIntoSuccess()) {
                    LOG.error(String.format("Execute copy sql status is not success, status: %s, response: %s",
                            responseContentObject.getStatus(), loadResponse));
                    throw new CopyIntoException(String.format("Execute copy sql error, load status:%s, response:%s",
                            responseContentObject.getStatus(), loadResponse));
                }
                LOG.info("Execute copy sql Response: {}", loadResponse);
            } catch (IOException e) {
                throw new CopyIntoException(e);
            }
        }
    }

    @Override
    public void commit(String message) throws Exception {
        frontend.requestFrontends((frontend, httpClient) -> {
            try {
                executeCopyInto(frontend, httpClient, message);
            } catch (OptionRequiredException | IOException | CopyIntoException e) {
                throw new RuntimeException("execute copy into failed", e);
            }
            return null;
        });
    }

    @Override
    public void abort(String message) throws Exception {
        // do nothing
    }
}
