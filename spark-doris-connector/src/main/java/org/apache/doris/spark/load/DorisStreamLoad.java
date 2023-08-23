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
import org.apache.doris.spark.util.ListUtils;
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
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
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
import java.util.stream.Collectors;

/**
 * DorisStreamLoad
 **/
public class DorisStreamLoad implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(DorisStreamLoad.class);

    private static final String NULL_VALUE = "\\N";
    private static final List<String> DORIS_SUCCESS_STATUS = new ArrayList<>(
            Arrays.asList("Success", "Publish Timeout"));
    private static final long cacheExpireTimeout = 4 * 60;
    private static String loadUrlPattern = "http://%s/api/%s/%s/_stream_load?";

    private static String abortUrlPattern = "http://%s/api/%s/%s/_stream_load_2pc?";
    private final LoadingCache<String, List<BackendV2.BackendRowV2>> cache;
    private final String fileType;
    private final String LINE_DELIMITER;
    private String FIELD_DELIMITER;
    private String user;
    private String passwd;
    private String loadUrlStr;
    private String db;
    private String tbl;
    private String authEncoded;
    private String columns;
    private String maxFilterRatio;
    private Map<String, String> streamLoadProp;
    private boolean readJsonByLine = false;

    public DorisStreamLoad(SparkSettings settings) {
        String[] dbTable = settings.getProperty(ConfigurationOptions.DORIS_TABLE_IDENTIFIER).split("\\.");
        this.db = dbTable[0];
        this.tbl = dbTable[1];
        this.user = settings.getProperty(ConfigurationOptions.DORIS_REQUEST_AUTH_USER);
        this.passwd = settings.getProperty(ConfigurationOptions.DORIS_REQUEST_AUTH_PASSWORD);
        this.authEncoded = getAuthEncoded(user, passwd);
        this.columns = settings.getProperty(ConfigurationOptions.DORIS_WRITE_FIELDS);
        this.maxFilterRatio = settings.getProperty(ConfigurationOptions.DORIS_MAX_FILTER_RATIO);
        this.streamLoadProp = getStreamLoadProp(settings);
        cache = CacheBuilder.newBuilder().expireAfterWrite(cacheExpireTimeout, TimeUnit.MINUTES)
                .build(new BackendCacheLoader(settings));
        fileType = streamLoadProp.getOrDefault("format", "csv");
        if ("csv".equals(fileType)) {
            FIELD_DELIMITER = escapeString(streamLoadProp.getOrDefault("column_separator", "\t"));
        } else if ("json".equalsIgnoreCase(fileType)) {
            readJsonByLine = Boolean.parseBoolean(streamLoadProp.getOrDefault("read_json_by_line", "false"));
            boolean stripOuterArray = Boolean.parseBoolean(streamLoadProp.getOrDefault("strip_outer_array", "false"));
            if (readJsonByLine && stripOuterArray) {
                throw new IllegalArgumentException(
                        "Only one of options 'read_json_by_line' and 'strip_outer_array' can be set to true");
            } else if (!readJsonByLine && !stripOuterArray) {
                LOG.info("set default json mode: strip_outer_array");
                streamLoadProp.put("strip_outer_array", "true");
            }
        }
        LINE_DELIMITER = escapeString(streamLoadProp.getOrDefault("line_delimiter", "\n"));
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
        httpPut.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + authEncoded);
        httpPut.setHeader(HttpHeaders.EXPECT, "100-continue");
        httpPut.setHeader(HttpHeaders.CONTENT_TYPE, "text/plain; charset=UTF-8");
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
            streamLoadProp.forEach(httpPut::setHeader);
        }
        return httpPut;
    }

    public static class LoadResponse {
        public int status;
        public String respMsg;
        public String respContent;

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

    public List<Integer> loadV2(List<List<Object>> rows, String[] dfColumns, Boolean enable2PC)
            throws StreamLoadException, JsonProcessingException {

        List<String> loadData = parseLoadData(rows, dfColumns);
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

    public int load(String value, Boolean enable2PC) throws StreamLoadException {

        String label = generateLoadLabel();

        LoadResponse loadResponse;
        int responseHttpStatus = -1;
        try (CloseableHttpClient httpClient = getHttpClient()) {
            String loadUrlStr = String.format(loadUrlPattern, getBackend(), db, tbl);
            LOG.debug("Stream load Request:{} ,Body:{}", loadUrlStr, value);
            // only to record the BE node in case of an exception
            this.loadUrlStr = loadUrlStr;

            HttpPut httpPut = getHttpPut(label, loadUrlStr, enable2PC);
            httpPut.setEntity(new StringEntity(value, StandardCharsets.UTF_8));
            HttpResponse httpResponse = httpClient.execute(httpPut);
            responseHttpStatus = httpResponse.getStatusLine().getStatusCode();
            String respMsg = httpResponse.getStatusLine().getReasonPhrase();
            String response = EntityUtils.toString(new BufferedHttpEntity(httpResponse.getEntity()),
                    StandardCharsets.UTF_8);
            loadResponse = new LoadResponse(responseHttpStatus, respMsg, response);
        } catch (IOException e) {
            e.printStackTrace();
            String err = "http request exception,load url : " + loadUrlStr
                    + ",failed to execute spark stream load with label: " + label;
            LOG.warn(err, e);
            loadResponse = new LoadResponse(responseHttpStatus, e.getMessage(), err);
        }

        if (loadResponse.status != HttpStatus.SC_OK) {
            LOG.info("Stream load Response HTTP Status Error:{}", loadResponse);
            // throw new StreamLoadException("stream load error: " + loadResponse.respContent);
            throw new StreamLoadException("stream load error");
        } else {
            ObjectMapper obj = new ObjectMapper();
            try {
                RespContent respContent = obj.readValue(loadResponse.respContent, RespContent.class);
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

    public void commit(int txnId) throws StreamLoadException {

        try (CloseableHttpClient client = getHttpClient()) {

            String backend = getBackend();
            String abortUrl = String.format(abortUrlPattern, backend, db, tbl);
            HttpPut httpPut = new HttpPut(abortUrl);
            httpPut.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + authEncoded);
            httpPut.setHeader(HttpHeaders.EXPECT, "100-continue");
            httpPut.setHeader(HttpHeaders.CONTENT_TYPE, "text/plain; charset=UTF-8");
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

            ObjectMapper mapper = new ObjectMapper();
            if (response.getEntity() != null) {
                String loadResult = EntityUtils.toString(response.getEntity());
                Map<String, String> res = mapper.readValue(loadResult, new TypeReference<HashMap<String, String>>() {
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
            httpPut.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + authEncoded);
            httpPut.setHeader(HttpHeaders.EXPECT, "100-continue");
            httpPut.setHeader(HttpHeaders.CONTENT_TYPE, "text/plain; charset=UTF-8");
            httpPut.setHeader("txn_operation", "abort");
            httpPut.setHeader("txn_id", String.valueOf(txnId));

            CloseableHttpResponse response = client.execute(httpPut);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200 || response.getEntity() == null) {
                LOG.warn("abort transaction response: " + response.getStatusLine().toString());
                throw new StreamLoadException("Fail to abort transaction " + txnId + " with url " + abortUrl);
            }

            ObjectMapper mapper = new ObjectMapper();
            String loadResult = EntityUtils.toString(response.getEntity());
            Map<String, String> res = mapper.readValue(loadResult, new TypeReference<HashMap<String, String>>() {
            });
            if (!"Success".equals(res.get("status"))) {
                if (ResponseUtil.isCommitted(res.get("msg"))) {
                    throw new StreamLoadException(
                            "try abort committed transaction, " + "do you recover from old savepoint?");
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
    private static class BackendCacheLoader extends CacheLoader<String, List<BackendV2.BackendRowV2>>
            implements Serializable {

        private final SparkSettings settings;

        public BackendCacheLoader(SparkSettings settings) {
            this.settings = settings;
        }

        @Override
        public List<BackendV2.BackendRowV2> load(String key) throws Exception {
            return RestService.getBackendRows(settings, LOG);
        }

    }

    private List<String> parseLoadData(List<List<Object>> rows, String[] dfColumns)
            throws StreamLoadException, JsonProcessingException {

        List<String> loadDataList;

        switch (fileType.toUpperCase()) {

            case "CSV":
                loadDataList = Collections.singletonList(rows.stream()
                        .map(row -> row.stream().map(DataUtil::handleColumnValue).map(Object::toString)
                                .collect(Collectors.joining(FIELD_DELIMITER)))
                        .collect(Collectors.joining(LINE_DELIMITER)));
                break;
            case "JSON":
                List<Map<Object, Object>> dataList = new ArrayList<>();
                try {
                    for (List<Object> row : rows) {
                        Map<Object, Object> dataMap = new HashMap<>();
                        if (dfColumns.length == row.size()) {
                            for (int i = 0; i < dfColumns.length; i++) {
                                dataMap.put(dfColumns[i], DataUtil.handleColumnValue(row.get(i)));
                            }
                        }
                        dataList.add(dataMap);
                    }
                } catch (Exception e) {
                    throw new StreamLoadException(
                            "The number of configured columns does not match the number of data columns.");
                }
                // splits large collections to normal collection to avoid
                // the "Requested array size exceeds VM limit" exception
                loadDataList = ListUtils.getSerializedList(dataList, readJsonByLine ? LINE_DELIMITER : null);
                break;
            default:
                throw new StreamLoadException(String.format("Unsupported file format in stream load: %s.", fileType));

        }

        return loadDataList;

    }

    private String generateLoadLabel() {

        Calendar calendar = Calendar.getInstance();
        return String.format("spark_streamload_%s%02d%02d_%02d%02d%02d_%s", calendar.get(Calendar.YEAR),
                calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH),
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

}
