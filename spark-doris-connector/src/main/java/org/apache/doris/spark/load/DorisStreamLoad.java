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
package org.apache.doris.spark.load;

import org.apache.doris.spark.cfg.ConfigurationOptions;
import org.apache.doris.spark.cfg.SparkSettings;
import org.apache.doris.spark.exception.StreamLoadException;
import org.apache.doris.spark.rest.RestService;
import org.apache.doris.spark.rest.models.BackendV2;
import org.apache.doris.spark.rest.models.RespContent;
import org.apache.doris.spark.util.DataUtil;
import org.apache.doris.spark.util.ResponseUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


/**
 * DorisStreamLoad
 **/
public class DorisStreamLoad implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DorisStreamLoad.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final static List<String> DORIS_SUCCESS_STATUS = new ArrayList<>(Arrays.asList("Success", "Publish Timeout"));

    private static final String loadUrlPattern = "http://%s/api/%s/%s/_stream_load?";

    private static final String abortUrlPattern = "http://%s/api/%s/%s/_stream_load_2pc?";

    private String loadUrlStr;
    private final String db;
    private final String tbl;
    private final String authEncoded;
    private final String columns;
    private final String maxFilterRatio;
    private final Map<String, String> streamLoadProp;
    private static final long cacheExpireTimeout = 4 * 60;
    private final LoadingCache<String, List<BackendV2.BackendRowV2>> cache;
    private final String fileType;
    private String FIELD_DELIMITER;
    private final String LINE_DELIMITER;
    private boolean readJsonByLine = false;

    private boolean streamingPassthrough = false;

    public DorisStreamLoad(SparkSettings settings) {
        String[] dbTable = settings.getProperty(ConfigurationOptions.DORIS_TABLE_IDENTIFIER).split("\\.");
        this.db = dbTable[0];
        this.tbl = dbTable[1];
        String user = settings.getProperty(ConfigurationOptions.DORIS_REQUEST_AUTH_USER);
        String passwd = settings.getProperty(ConfigurationOptions.DORIS_REQUEST_AUTH_PASSWORD);
        this.authEncoded = getAuthEncoded(user, passwd);
        this.columns = settings.getProperty(ConfigurationOptions.DORIS_WRITE_FIELDS);
        this.maxFilterRatio = settings.getProperty(ConfigurationOptions.DORIS_MAX_FILTER_RATIO);
        this.streamLoadProp = getStreamLoadProp(settings);
        cache = CacheBuilder.newBuilder().expireAfterWrite(cacheExpireTimeout, TimeUnit.MINUTES).build(new BackendCacheLoader(settings));
        fileType = streamLoadProp.getOrDefault("format", "csv");
        if ("csv".equals(fileType)) {
            FIELD_DELIMITER = escapeString(streamLoadProp.getOrDefault("column_separator", "\t"));
        } else if ("json".equalsIgnoreCase(fileType)) {
            streamLoadProp.put("read_json_by_line", "true");
        }
        LINE_DELIMITER = escapeString(streamLoadProp.getOrDefault("line_delimiter", "\n"));
        this.streamingPassthrough = settings.getBooleanProperty(ConfigurationOptions.DORIS_SINK_STREAMING_PASSTHROUGH,
                ConfigurationOptions.DORIS_SINK_STREAMING_PASSTHROUGH_DEFAULT);
    }

    public String getLoadUrlStr() {
        if (StringUtils.isEmpty(loadUrlStr)) {
            return "";
        }
        return loadUrlStr;
    }

    private CloseableHttpClient getHttpClient() {
        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create().disableRedirectHandling();
        return httpClientBuilder.build();
    }

    private HttpPut getHttpPut(String label, String loadUrlStr, Boolean enable2PC) {
        HttpPut httpPut = new HttpPut(loadUrlStr);
        addCommonHeader(httpPut);
        httpPut.setHeader("label", label);
        if (StringUtils.isNotBlank(columns)) {
            httpPut.setHeader("columns", columns);
        }
        if (StringUtils.isNotBlank(maxFilterRatio)) {
            httpPut.setHeader("max_filter_ratio", maxFilterRatio);
        }
        if (enable2PC) {
            httpPut.setHeader("two_phase_commit", "true");
        }
        if (MapUtils.isNotEmpty(streamLoadProp)) {
            streamLoadProp.forEach((k, v) -> {
                if (!"strip_outer_array".equalsIgnoreCase(k)) {
                    httpPut.setHeader(k, v);
                }
            });
        }
        return httpPut;
    }

    public static class LoadResponse {
        public int status;
        public String respMsg;
        public String respContent;

        public LoadResponse(HttpResponse response) throws IOException {
            this.status = response.getStatusLine().getStatusCode();
            this.respMsg = response.getStatusLine().getReasonPhrase();
            this.respContent = EntityUtils.toString(new BufferedHttpEntity(response.getEntity()), StandardCharsets.UTF_8);
        }

        public LoadResponse(int status, String respMsg, String respContent) {
            this.status = status;
            this.respMsg = respMsg;
            this.respContent = respContent;
        }

        @Override
        public String toString() {
            return "status: " + status + ", resp msg: " + respMsg + ", resp content: " + respContent;
        }
    }

    public int loadV2(List<Row> rows, String[] dfColumns, Boolean enable2PC) throws StreamLoadException, JsonProcessingException {

        String data = parseLoadData(rows, dfColumns);

        String label = generateLoadLabel();
        LoadResponse loadResponse;
        try (CloseableHttpClient httpClient = getHttpClient()) {
            String loadUrlStr = String.format(loadUrlPattern, getBackend(), db, tbl);
            LOG.debug("Stream load Request:{} ,Body:{}", loadUrlStr, data);
            // only to record the BE node in case of an exception
            this.loadUrlStr = loadUrlStr;
            HttpPut httpPut = getHttpPut(label, loadUrlStr, enable2PC);
            RowInputStream rowInputStream = RowInputStream.newBuilder(rows.iterator())
                    .format(fileType)
                    .sep(FIELD_DELIMITER)
                    .delim(LINE_DELIMITER)
                    .columns(dfColumns).build();
            httpPut.setEntity(new InputStreamEntity(rowInputStream));
            HttpResponse httpResponse = httpClient.execute(httpPut);
            loadResponse = new LoadResponse(httpResponse);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (loadResponse.status != HttpStatus.SC_OK) {
            LOG.info("Stream load Response HTTP Status Error:{}", loadResponse);
            // throw new StreamLoadException("stream load error: " + loadResponse.respContent);
            throw new StreamLoadException("stream load error");
        } else {
            try {
                RespContent respContent = MAPPER.readValue(loadResponse.respContent, RespContent.class);
                if (!DORIS_SUCCESS_STATUS.contains(respContent.getStatus())) {
                    LOG.error("Stream load Response RES STATUS Error:{}", loadResponse);
                    throw new StreamLoadException("stream load error");
                }
                LOG.info("Stream load Response:{}", loadResponse);
                return respContent.getTxnId();
            } catch (IOException e) {
                throw new StreamLoadException(e);
            }
        }

    }

    public List<Integer> loadStream(List<List<Object>> rows, String[] dfColumns, Boolean enable2PC)
            throws StreamLoadException, JsonProcessingException {

        List<String> loadData;

        if (this.streamingPassthrough) {
            handleStreamPassThrough();
            loadData = passthrough(rows);
        } else {
            loadData = parseLoadData(rows, dfColumns);
        }

        List<Integer> txnIds = new ArrayList<>(loadData.size());

        try {
            for (String data : loadData) {
                txnIds.add(load(data, enable2PC));
            }
        } catch (StreamLoadException e) {
            if (enable2PC && !txnIds.isEmpty()) {
                LOG.error("load batch failed, abort previously pre-committed transactions");
                for (Integer txnId : txnIds) {
                    abort(txnId);
                }
            }
            throw e;
        }

        return txnIds;

    }

    public void commit(int txnId) throws StreamLoadException {

        try (CloseableHttpClient client = getHttpClient()) {

            String backend = getBackend();
            String abortUrl = String.format(abortUrlPattern, backend, db, tbl);
            HttpPut httpPut = new HttpPut(abortUrl);
            addCommonHeader(httpPut);
            httpPut.setHeader("txn_operation", "commit");
            httpPut.setHeader("txn_id", String.valueOf(txnId));

            CloseableHttpResponse response = client.execute(httpPut);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200 || response.getEntity() == null) {
                LOG.warn("commit transaction response: " + response.getStatusLine().toString());
                throw new StreamLoadException("Fail to commit transaction " + txnId + " with url " + abortUrl);
            }

            statusCode = response.getStatusLine().getStatusCode();
            String reasonPhrase = response.getStatusLine().getReasonPhrase();
            if (statusCode != 200) {
                LOG.warn("commit failed with {}, reason {}", backend, reasonPhrase);
                throw new StreamLoadException("stream load error: " + reasonPhrase);
            }

            if (response.getEntity() != null) {
                String loadResult = EntityUtils.toString(response.getEntity());
                Map<String, String> res = MAPPER.readValue(loadResult, new TypeReference<HashMap<String, String>>() {
                });
                if (res.get("status").equals("Fail") && !ResponseUtil.isCommitted(res.get("msg"))) {
                    throw new StreamLoadException("Commit failed " + loadResult);
                } else {
                    LOG.info("load result {}", loadResult);
                }
            }

        } catch (IOException e) {
            throw new StreamLoadException(e);
        }

    }

    public void abort(int txnId) throws StreamLoadException {

        LOG.info("start abort transaction {}.", txnId);

        try (CloseableHttpClient client = getHttpClient()) {
            String abortUrl = String.format(abortUrlPattern, getBackend(), db, tbl);
            HttpPut httpPut = new HttpPut(abortUrl);
            addCommonHeader(httpPut);
            httpPut.setHeader("txn_operation", "abort");
            httpPut.setHeader("txn_id", String.valueOf(txnId));

            CloseableHttpResponse response = client.execute(httpPut);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200 || response.getEntity() == null) {
                LOG.warn("abort transaction response: " + response.getStatusLine().toString());
                throw new StreamLoadException("Fail to abort transaction " + txnId + " with url " + abortUrl);
            }

            String loadResult = EntityUtils.toString(response.getEntity());
            Map<String, String> res = MAPPER.readValue(loadResult, new TypeReference<HashMap<String, String>>() {
            });
            if (!"Success".equals(res.get("status"))) {
                if (ResponseUtil.isCommitted(res.get("msg"))) {
                    throw new StreamLoadException("try abort committed transaction, " + "do you recover from old savepoint?");
                }
                LOG.warn("Fail to abort transaction. txnId: {}, error: {}", txnId, res.get("msg"));
            }

        } catch (IOException e) {
            throw new StreamLoadException(e);
        }

        LOG.info("abort transaction {} succeed.", txnId);

    }

    public Map<String, String> getStreamLoadProp(SparkSettings sparkSettings) {
        Map<String, String> streamLoadPropMap = new HashMap<>();
        Properties properties = sparkSettings.asProperties();
        for (String key : properties.stringPropertyNames()) {
            if (key.contains(ConfigurationOptions.STREAM_LOAD_PROP_PREFIX)) {
                String subKey = key.substring(ConfigurationOptions.STREAM_LOAD_PROP_PREFIX.length());
                streamLoadPropMap.put(subKey, properties.getProperty(key));
            }
        }
        return streamLoadPropMap;
    }

    private String getBackend() {
        try {
            // get backends from cache
            List<BackendV2.BackendRowV2> backends = cache.get("backends");
            Collections.shuffle(backends);
            BackendV2.BackendRowV2 backend = backends.get(0);
            return backend.getIp() + ":" + backend.getHttpPort();
        } catch (ExecutionException e) {
            throw new RuntimeException("get backends info fail", e);
        }
    }

    private static String getAuthEncoded(String user, String passwd) {
        String raw = String.format("%s:%s", user, passwd);
        return Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * serializable be cache loader
     */
    private static class BackendCacheLoader extends CacheLoader<String, List<BackendV2.BackendRowV2>> implements Serializable {

        private final SparkSettings settings;

        public BackendCacheLoader(SparkSettings settings) {
            this.settings = settings;
        }

        @Override
        public List<BackendV2.BackendRowV2> load(String key) throws Exception {
            return RestService.getBackendRows(settings, LOG);
        }

    }

    private String parseLoadData(List<Row> rows, String[] dfColumns) throws StreamLoadException, JsonProcessingException {

        if (dfColumns.length != rows.get(0).size()) {
            return "";
        }

        switch (fileType.toUpperCase()) {
            case "CSV":
                return DataUtil.rowsToCsv(rows, FIELD_DELIMITER, LINE_DELIMITER);
            case "JSON":
                return DataUtil.rowsToJson(rows, dfColumns, readJsonByLine ? LINE_DELIMITER : null);
            default:
                throw new StreamLoadException(String.format("Unsupported file format in stream load: %s.", fileType));
        }

    }

    private String generateLoadLabel() {

        Calendar calendar = Calendar.getInstance();
        return String.format("spark_streamload_%s%02d%02d_%02d%02d%02d_%s",
                calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH),
                calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND),
                UUID.randomUUID().toString().replaceAll("-", ""));

    }

    private String escapeString(String hexData) {
        if (hexData.startsWith("\\x") || hexData.startsWith("\\X")) {
            try {
                hexData = hexData.substring(2);
                StringBuilder stringBuilder = new StringBuilder();
                for (int i = 0; i < hexData.length(); i += 2) {
                    String hexByte = hexData.substring(i, i + 2);
                    int decimal = Integer.parseInt(hexByte, 16);
                    char character = (char) decimal;
                    stringBuilder.append(character);
                }
                return stringBuilder.toString();
            } catch (Exception e) {
                throw new RuntimeException("escape column_separator or line_delimiter error.{}", e);
            }
        }
        return hexData;
    }

    private void addCommonHeader(HttpRequestBase httpReq) {
        httpReq.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + authEncoded);
        httpReq.setHeader(HttpHeaders.EXPECT, "100-continue");
        httpReq.setHeader(HttpHeaders.CONTENT_TYPE, "text/plain; charset=UTF-8");
    }

    private void handleStreamPassThrough() {

        if ("json".equalsIgnoreCase(fileType)) {
            LOG.info("handle stream pass through, force set read_json_by_line is true for json format");
            streamLoadProp.put("read_json_by_line", "true");
            streamLoadProp.remove("strip_outer_array");
        }

    }

    private List<String> passthrough(List<List<Object>> values) {
        return values.stream().map(list -> list.get(0).toString()).collect(Collectors.toList());
    }

}
