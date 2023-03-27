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
package org.apache.doris.spark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.spark.cfg.ConfigurationOptions;
import org.apache.doris.spark.cfg.SparkSettings;
import org.apache.doris.spark.exception.DorisException;
import org.apache.doris.spark.exception.StreamLoadException;
import org.apache.doris.spark.rest.RestService;
import org.apache.doris.spark.rest.models.BackendV2;
import org.apache.doris.spark.rest.models.RespContent;
import org.apache.doris.spark.util.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * DorisStreamLoad
 **/
public class DorisStreamLoad implements Serializable {
    private String FIELD_DELIMITER;
    private String LINE_DELIMITER;
    private String NULL_VALUE = "\\N";

    private static final Logger LOG = LoggerFactory.getLogger(DorisStreamLoad.class);

    private final static List<String> DORIS_SUCCESS_STATUS = new ArrayList<>(Arrays.asList("Success", "Publish Timeout"));
    private static String loadUrlPattern = "http://%s/api/%s/%s/_stream_load?";
    private String user;
    private String passwd;
    private String loadUrlStr;
    private String db;
    private String tbl;
    private String authEncoding;
    private String columns;
    private String[] dfColumns;
    private String maxFilterRatio;
    private Map<String, String> streamLoadProp;
    private static final long cacheExpireTimeout = 4 * 60;
    private LoadingCache<String, List<BackendV2.BackendRowV2>> cache;
    private String fileType;

    public DorisStreamLoad(String hostPort, String db, String tbl, String user, String passwd) {
        this.db = db;
        this.tbl = tbl;
        this.user = user;
        this.passwd = passwd;
        this.loadUrlStr = String.format(loadUrlPattern, hostPort, db, tbl);
        this.authEncoding = Base64.getEncoder().encodeToString(String.format("%s:%s", user, passwd).getBytes(StandardCharsets.UTF_8));
    }

    public DorisStreamLoad(SparkSettings settings) throws IOException, DorisException {
        String[] dbTable = settings.getProperty(ConfigurationOptions.DORIS_TABLE_IDENTIFIER).split("\\.");
        this.db = dbTable[0];
        this.tbl = dbTable[1];
        this.user = settings.getProperty(ConfigurationOptions.DORIS_REQUEST_AUTH_USER);
        this.passwd = settings.getProperty(ConfigurationOptions.DORIS_REQUEST_AUTH_PASSWORD);
        this.authEncoding = Base64.getEncoder().encodeToString(String.format("%s:%s", user, passwd).getBytes(StandardCharsets.UTF_8));
        this.columns = settings.getProperty(ConfigurationOptions.DORIS_WRITE_FIELDS);

        this.maxFilterRatio = settings.getProperty(ConfigurationOptions.DORIS_MAX_FILTER_RATIO);
        this.streamLoadProp = getStreamLoadProp(settings);
        cache = CacheBuilder.newBuilder()
                .expireAfterWrite(cacheExpireTimeout, TimeUnit.MINUTES)
                .build(new BackendCacheLoader(settings));
        fileType = this.streamLoadProp.get("format") == null ? "csv" : this.streamLoadProp.get("format");
        if (fileType.equals("csv")){
            FIELD_DELIMITER = this.streamLoadProp.get("column_separator") == null ? "\t" : this.streamLoadProp.get("column_separator");
            LINE_DELIMITER = this.streamLoadProp.get("line_delimiter") == null ? "\n" : this.streamLoadProp.get("line_delimiter");
        }
    }

    public DorisStreamLoad(SparkSettings settings, String[] dfColumns) throws IOException, DorisException {
        String[] dbTable = settings.getProperty(ConfigurationOptions.DORIS_TABLE_IDENTIFIER).split("\\.");
        this.db = dbTable[0];
        this.tbl = dbTable[1];
        this.user = settings.getProperty(ConfigurationOptions.DORIS_REQUEST_AUTH_USER);
        this.passwd = settings.getProperty(ConfigurationOptions.DORIS_REQUEST_AUTH_PASSWORD);


        this.authEncoding = Base64.getEncoder().encodeToString(String.format("%s:%s", user, passwd).getBytes(StandardCharsets.UTF_8));
        this.columns = settings.getProperty(ConfigurationOptions.DORIS_WRITE_FIELDS);
        this.dfColumns = dfColumns;

        this.maxFilterRatio = settings.getProperty(ConfigurationOptions.DORIS_MAX_FILTER_RATIO);
        this.streamLoadProp = getStreamLoadProp(settings);
        cache = CacheBuilder.newBuilder()
                .expireAfterWrite(cacheExpireTimeout, TimeUnit.MINUTES)
                .build(new BackendCacheLoader(settings));
        fileType = this.streamLoadProp.get("format") == null ? "csv" : this.streamLoadProp.get("format");
        if ("csv".equals(fileType)) {
            FIELD_DELIMITER = this.streamLoadProp.get("column_separator") == null ? "\t" : this.streamLoadProp.get("column_separator");
            LINE_DELIMITER = this.streamLoadProp.get("line_delimiter") == null ? "\n" : this.streamLoadProp.get("line_delimiter");
        }
    }

    public String getLoadUrlStr() {
        if (StringUtils.isEmpty(loadUrlStr)) {
            return "";
        }
        return loadUrlStr;
    }

    private HttpURLConnection getConnection(String urlStr, String label) throws IOException {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setInstanceFollowRedirects(false);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Authorization", "Basic " + authEncoding);
        conn.addRequestProperty("Expect", "100-continue");
        conn.addRequestProperty("Content-Type", "text/plain; charset=UTF-8");
        conn.addRequestProperty("label", label);
        if (columns != null && !columns.equals("")) {
            conn.addRequestProperty("columns", columns);
        }

        if (maxFilterRatio != null && !maxFilterRatio.equals("")) {
            conn.addRequestProperty("max_filter_ratio", maxFilterRatio);
        }

        conn.setDoOutput(true);
        conn.setDoInput(true);
        if (streamLoadProp != null) {
            streamLoadProp.forEach((k, v) -> {
                if ("read_json_by_line".equals(k)) {
                    return;
                }
                conn.addRequestProperty(k, v);
            });
        }
        if (fileType.equals("json")) {
            conn.addRequestProperty("strip_outer_array", "true");
        }
        return conn;
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
            return "status: " + status +
                    ", resp msg: " + respMsg +
                    ", resp content: " + respContent;
        }
    }

    public String listToString(List<List<Object>> rows) {
        return rows.stream().map(row ->
                row.stream().map(field ->
                        (field == null) ? NULL_VALUE : field.toString()
                ).collect(Collectors.joining(FIELD_DELIMITER))
        ).collect(Collectors.joining(LINE_DELIMITER));
    }


    public void loadV2(List<List<Object>> rows) throws StreamLoadException, JsonProcessingException {
        if (fileType.equals("csv")) {
            load(listToString(rows));
        } else if(fileType.equals("json")) {
            List<Map<Object, Object>> dataList = new ArrayList<>();
            try {
                for (List<Object> row : rows) {
                    Map<Object, Object> dataMap = new HashMap<>();
                    if (dfColumns.length == row.size()) {
                        for (int i = 0; i < dfColumns.length; i++) {
                            dataMap.put(dfColumns[i], row.get(i));
                        }
                    }
                    dataList.add(dataMap);
                }
            } catch (Exception e) {
                throw new StreamLoadException("The number of configured columns does not match the number of data columns.");
            }
            // splits large collections to normal collection to avoid the "Requested array size exceeds VM limit" exception
            List<String> serializedList = ListUtils.getSerializedList(dataList);
            for (String serializedRows : serializedList) {
                load(serializedRows);
            }
        } else {
            throw new StreamLoadException("Not supoort the file format in stream load.");
        }
    }

    public void load(String value) throws StreamLoadException {
        LoadResponse loadResponse = loadBatch(value);
        if (loadResponse.status != 200) {
            LOG.info("Streamload Response HTTP Status Error:{}", loadResponse);
            throw new StreamLoadException("stream load error: " + loadResponse.respContent);
        } else {
            ObjectMapper obj = new ObjectMapper();
            try {
                RespContent respContent = obj.readValue(loadResponse.respContent, RespContent.class);
                if (!DORIS_SUCCESS_STATUS.contains(respContent.getStatus())) {
                    LOG.error("Streamload Response RES STATUS Error:{}", loadResponse);
                    throw new StreamLoadException("stream load error: " + loadResponse);
                }
                LOG.info("Streamload Response:{}", loadResponse);
            } catch (IOException e) {
                throw new StreamLoadException(e);
            }
        }
    }

    private LoadResponse loadBatch(String value) {
        Calendar calendar = Calendar.getInstance();
        String label = String.format("spark_streamload_%s%02d%02d_%02d%02d%02d_%s",
                calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH),
                calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND),
                UUID.randomUUID().toString().replaceAll("-", ""));

        String loadUrlStr = String.format(loadUrlPattern, getBackend(), db, tbl);
        LOG.debug("Streamload Request:{} ,Body:{}", loadUrlStr, value);
        //only to record the BE node in case of an exception
        this.loadUrlStr = loadUrlStr;

        HttpURLConnection beConn = null;
        int status = -1;
        try {
            // build request and send to new be location
            beConn = getConnection(loadUrlStr, label);
            // send data to be
            try (OutputStream beConnOutputStream = new BufferedOutputStream(beConn.getOutputStream())) {
                IOUtils.write(value, beConnOutputStream, StandardCharsets.UTF_8);
            }

            // get respond
            status = beConn.getResponseCode();
            String respMsg = beConn.getResponseMessage();
            String response;
            try (InputStream beConnInputStream = beConn.getInputStream()) {
                response = IOUtils.toString(beConnInputStream, StandardCharsets.UTF_8);
            }
            return new LoadResponse(status, respMsg, response);

        } catch (Exception e) {
            e.printStackTrace();
            String err = "http request exception,load url : " + loadUrlStr + ",failed to execute spark streamload with label: " + label;
            LOG.warn(err, e);
            return new LoadResponse(status, e.getMessage(), err);
        } finally {
            if (beConn != null) {
                beConn.disconnect();
            }
        }
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
            //get backends from cache
            List<BackendV2.BackendRowV2> backends = cache.get("backends");
            Collections.shuffle(backends);
            BackendV2.BackendRowV2 backend = backends.get(0);
            return backend.getIp() + ":" + backend.getHttpPort();
        } catch (ExecutionException e) {
            throw new RuntimeException("get backends info fail", e);
        }
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

}
