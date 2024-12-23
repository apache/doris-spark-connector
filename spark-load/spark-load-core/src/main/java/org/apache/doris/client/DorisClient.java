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

package org.apache.doris.client;

import org.apache.doris.common.Constants;
import org.apache.doris.common.LoadInfo;
import org.apache.doris.common.ResponseEntity;
import org.apache.doris.common.meta.LoadInfoResponse;
import org.apache.doris.common.meta.LoadMeta;
import org.apache.doris.exception.SparkLoadException;
import org.apache.doris.util.HttpUtils;
import org.apache.doris.util.JsonUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DorisClient {

    private static volatile FeClient FE;
    private static BeClient BE;

    public static FeClient getFeClient(String feAddresses, String user, String password) {
        if (FE == null) {
            synchronized (FeClient.class) {
                if (FE == null) {
                    FE = new FeClient(feAddresses, user, password);
                }
            }
        }
        return FE;
    }

    public static class FeClient {

        public static final String BASE_URL = "http://%s%s";

        public static final String INGESTION_LOAD_URL_PATTERN = "/api/ingestion_load/%s/%s/%s";

        public static final String CREATE_ACTION = "_create";

        public static final String UPDATE_ACTION = "_update";

        public static final String GET_LOAD_INFO = "/api/%s/_load_info";

        public static final String GET_DDL = "/api/_get_ddl";

        private final List<String> feNodes;

        private final String auth;

        public FeClient(String feAddresses, String user, String password) {
            this.feNodes = parseFeNodes(feAddresses);
            this.auth = parseAuth(user, password);
        }

        private List<String> parseFeNodes(String feAddresses) {
            if (StringUtils.isBlank(feAddresses)) {
                throw new IllegalArgumentException("feAddresses is empty");
            }
            String[] feArr = feAddresses.split(",");
            if (Arrays.stream(feArr).map(x -> x.split(":"))
                    .anyMatch(x -> x.length != 2 || x[0].isEmpty() || x[1].isEmpty())) {
                throw new IllegalArgumentException("feAddresses contains invalid format, " + feAddresses);
            }
            return Arrays.stream(feArr).collect(Collectors.toList());
        }

        private String parseAuth(String user, String password) {
            return Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
        }

        public LoadMeta createIngestionLoad(String db, Map<String, List<String>> tableToPartition, String label,
                                            Map<String, String> properties) throws SparkLoadException {
            try {
                String path = String.format(INGESTION_LOAD_URL_PATTERN, Constants.DEFAULT_CATALOG, db, CREATE_ACTION);
                HttpPost httpPost = new HttpPost();
                addCommonHeaders(httpPost);
                Map<String, Object> params = new HashMap<>();
                params.put("label", label);
                params.put("tableToPartition", tableToPartition);
                params.put("properties", properties);
                httpPost.setEntity(new StringEntity(JsonUtils.writeValueAsString(params)));
                String content = executeRequest(httpPost, path, null);
                if (StringUtils.isBlank(content)) {
                    throw new SparkLoadException(String.format("request create load failed, path: %s", path));
                }
                ResponseEntity res = JsonUtils.readValue(content, ResponseEntity.class);
                if (res.getCode() != 0) {
                    throw new SparkLoadException(String.format("create load failed, code: %d, msg: %s, reason: %s",
                            res.getCode(), res.getMsg(), res.getData().isNull() ? null : res.getData().asText()));
                }
                return JsonUtils.readValue(res.getData().traverse(), LoadMeta.class);
            } catch (IOException | URISyntaxException e) {
                throw new SparkLoadException("create spark load failed", e);
            }
        }

        private void addCommonHeaders(HttpRequestBase req) {
            req.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + auth);
        }

        private String executeRequest(HttpRequestBase req, String apiPath, Map<String, String> params)
                throws IOException, URISyntaxException {
            IOException ex = null;
            try (CloseableHttpClient client = HttpUtils.getClient()) {
                for (String feNode : feNodes) {
                    String url = String.format(BASE_URL, feNode, apiPath);
                    URIBuilder uriBuilder = new URIBuilder(URI.create(url));
                    if (params != null && !params.isEmpty()) {
                        params.forEach(uriBuilder::addParameter);
                    }
                    req.setURI(uriBuilder.build());
                    addCommonHeaders(req);
                    CloseableHttpResponse res;
                    try {
                        res = client.execute(req);
                    } catch (IOException e) {
                        ex = e;
                        continue;
                    }
                    if (res.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                        continue;
                    }
                    return HttpUtils.getEntityContent(res.getEntity());
                }
            }
            if (ex != null) {
                throw ex;
            }
            return null;
        }

        public void updateIngestionLoad(String db, Long loadId, Map<String, String> statusInfo)
                throws SparkLoadException {

            String path = String.format(INGESTION_LOAD_URL_PATTERN, Constants.DEFAULT_CATALOG, db, UPDATE_ACTION);
            HttpPost httpPost = new HttpPost();
            addCommonHeaders(httpPost);
            Map<String, Object> params = new HashMap<>();
            params.put("loadId", loadId);
            params.put("statusInfo", statusInfo);
            try {
                httpPost.setEntity(new StringEntity(JsonUtils.writeValueAsString(params)));
                String content = executeRequest(httpPost, path, null);
                if (StringUtils.isBlank(content)) {
                    throw new SparkLoadException(String.format("request update load failed, path: %s", path));
                }
                ResponseEntity res = JsonUtils.readValue(content, ResponseEntity.class);
                if (res.getCode() != 0) {
                    throw new SparkLoadException(String.format("update load failed, code: %d, msg: %s, reason: %s",
                            res.getCode(), res.getMsg(), res.getData().isNull() ? null : res.getData().asText()));
                }
            } catch (IOException | URISyntaxException e) {
                throw new SparkLoadException("update load failed", e);
            }

        }

        public LoadInfo getLoadInfo(String db, String label) throws SparkLoadException {

            String path = String.format(GET_LOAD_INFO, db);
            HttpGet httpGet = new HttpGet();
            addCommonHeaders(httpGet);
            try {
                Map<String, String> params = new HashMap<>();
                params.put("label", label);
                String content = executeRequest(httpGet, path, params);
                if (StringUtils.isBlank(content)) {
                    throw new SparkLoadException(String.format("request get load info failed, path: %s", path));
                }
                LoadInfoResponse res = JsonUtils.readValue(content, LoadInfoResponse.class);
                if (!"ok".equalsIgnoreCase(res.getStatus())) {
                    throw new SparkLoadException(String.format("get load info failed, status: %s, msg: %s, jobInfo: %s",
                            res.getStatus(), res.getMsg(), JsonUtils.writeValueAsString(res.getJobInfo())));
                }
                return res.getJobInfo();
            } catch (IOException | URISyntaxException e) {
                throw new SparkLoadException("get load info failed", e);
            }

        }

        public String getDDL(String db, String table) throws SparkLoadException {

            HttpGet httpGet = new HttpGet();
            addCommonHeaders(httpGet);
            try {
                Map<String, String> params = new HashMap<>();
                params.put("db", db);
                params.put("table", table);
                String content = executeRequest(httpGet, GET_DDL, params);
                if (StringUtils.isBlank(content)) {
                    throw new SparkLoadException(String.format("request get ddl failed, path: %s", GET_DDL));
                }
                ResponseEntity res = JsonUtils.readValue(content, ResponseEntity.class);
                if (res.getCode() != 0 || !res.getData().has("create_table")
                        || res.getData().get("create_table").isEmpty()) {
                    throw new SparkLoadException(String.format("get ddl failed, status: %s, msg: %s, data: %s",
                            res.getCode(), res.getMsg(), JsonUtils.writeValueAsString(res.getData())));
                }
                return res.getData().get("create_table").get(0).asText();
            } catch (IOException | URISyntaxException e) {
                throw new SparkLoadException("get ddl failed", e);
            }

        }

    }

    private static class BeClient {

    }

}
