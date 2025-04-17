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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.doris.spark.client.entity.Backend;
import org.apache.doris.spark.client.entity.Frontend;
import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.config.DorisOptions;
import org.apache.doris.spark.exception.DorisException;
import org.apache.doris.spark.exception.OptionRequiredException;
import org.apache.doris.spark.rest.models.Field;
import org.apache.doris.spark.rest.models.QueryPlan;
import org.apache.doris.spark.rest.models.Schema;
import org.apache.doris.spark.util.HttpUtils;
import org.apache.doris.spark.util.URLs;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DorisFrontendClient implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DorisFrontendClient.class);

    private static final ObjectMapper MAPPER = JsonMapper.builder().build();

    private final DorisConfig config;
    private final String username;
    private final String password;
    private final List<Frontend> frontends;
    private final boolean isHttpsEnabled;
    private transient CloseableHttpClient httpClient;

    public DorisFrontendClient() {
        this.config = null;
        this.username = null;
        this.password = null;
        this.httpClient = null;
        this.isHttpsEnabled = false;
        this.frontends = Collections.emptyList();
    }

    public DorisFrontendClient(DorisConfig config) throws Exception {
        this.config = config;
        this.username = config.getValue(DorisOptions.DORIS_USER);
        this.password = config.getValue(DorisOptions.DORIS_PASSWORD);
        this.isHttpsEnabled = config.getValue(DorisOptions.DORIS_ENABLE_HTTPS);
        this.frontends = initFrontends(config);
    }

    private List<Frontend> initFrontends(DorisConfig config) throws Exception {
        String frontendNodes = config.getValue(DorisOptions.DORIS_FENODES);
        String[] frontendNodeArray = frontendNodes.split(",");
        if (config.getValue(DorisOptions.DORIS_FE_AUTO_FETCH)) {
            Exception ex = null;
            List<Frontend> frontendList = null;
            for (String frontendNode : frontendNodeArray) {
                String[] nodeDetails = frontendNode.split(":");
                try {
                    List<Frontend> list = Collections.singletonList(new Frontend(nodeDetails[0], nodeDetails.length > 1 ? Integer.parseInt(nodeDetails[1]) : -1));
                    frontendList = requestFrontends(list, (frontend, client) -> {
                        String feReqURL = URLs.getFrontEndNodes(frontend.getHost(), frontend.getHttpPort(), isHttpsEnabled);
                        HttpGet httpGet = new HttpGet(feReqURL);
                        HttpUtils.setAuth(httpGet, username, password);
                        JsonNode dataNode;
                        try {
                            HttpResponse response = client.execute(httpGet);
                            dataNode = extractDataFromResponse(response, feReqURL);
                        } catch (IOException e) {
                            throw new RuntimeException("fetch fe failed", e);
                        }
                        ArrayNode columnNames = (ArrayNode) dataNode.get("columnNames");
                        ArrayNode rows = (ArrayNode) dataNode.get("rows");
                        return parseFrontends(columnNames, rows);
                    });
                } catch (Exception e) {
                    LOG.warn("fetch fe request on {} failed, err: {}", frontendNode, e.getMessage());
                    ex = e;
                }
            }
            if (frontendList == null || frontendList.isEmpty()) {
                if (ex == null) {
                    throw new DorisException("frontend init fetch failed, empty frontend list");
                }
                throw new DorisException("frontend init fetch failed", ex);
            }
            return frontendList;
        } else {
            int queryPort = config.contains(DorisOptions.DORIS_QUERY_PORT) ?
                    config.getValue(DorisOptions.DORIS_QUERY_PORT) : -1;
            int flightSqlPort = config.contains(DorisOptions.DORIS_READ_FLIGHT_SQL_PORT) ?
                    config.getValue(DorisOptions.DORIS_READ_FLIGHT_SQL_PORT) : -1;
            return Arrays.stream(frontendNodeArray)
                    .map(node -> {
                        String[] nodeParts = node.split(":");
                        return new Frontend(nodeParts[0], nodeParts.length > 1 ? Integer.parseInt(nodeParts[1]) : -1, queryPort, flightSqlPort);
                    })
                    .collect(Collectors.toList());
        }
    }

    public <T> T requestFrontends(BiFunction<Frontend, CloseableHttpClient, T> reqFunc) throws Exception {
        return requestFrontends(frontends, reqFunc);
    }

    private <T> T requestFrontends(List<Frontend> frontEnds, BiFunction<Frontend, CloseableHttpClient, T> reqFunc) throws Exception {
        if (httpClient == null) {
            httpClient = HttpUtils.getHttpClient(config);
        }
        Exception ex = null;
        for (Frontend frontEnd : frontEnds) {
            try {
                return reqFunc.apply(frontEnd, httpClient);
            } catch (Exception e) {
                LOG.warn("fe http request on {} failed, err: {}", frontEnd.hostHttpPortString(), e.getMessage());
                ex = e;
            }
        }
        throw ex;
    }

    public <T> T queryFrontends(Function<Connection, T> function) throws Exception {
        Exception ex = null;
        for (Frontend frontEnd : frontends) {
            if (frontEnd.getQueryPort() == -1) {
                ex = new OptionRequiredException(DorisOptions.DORIS_QUERY_PORT.getName());
                break;
            }
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
            } catch (ClassNotFoundException e) {
                Class.forName("com.mysql.jdbc.Driver");
            }
            try (Connection conn = DriverManager.getConnection("jdbc:mysql://" + frontEnd.getHost() + ":" + frontEnd.getQueryPort(), username, password)) {
                return function.apply(conn);
            } catch (SQLException e) {
                LOG.warn("fe jdbc query on {} failed, err: {}", frontEnd.hostQueryPortString(), e.getMessage());
                ex = e;
            }
        }
        throw ex;
    }

    public List<Pair<String[], String>> listTables(String[] databases) throws Exception {
        return queryFrontends(conn -> {
            String where = databases.length == 1 ? " WHERE TABLE_SCHEMA = '" + databases[0] + "'" : "";
            String sql = "SELECT TABLE_NAME FROM `information_schema`.`tables`" + where;
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql);
                 ResultSet resultSet = preparedStatement.executeQuery()) {
                List<Pair<String[], String>> result = new ArrayList<>();
                while (resultSet.next()) {
                    result.add(Pair.of(databases, resultSet.getString(1)));
                }
                return result;
            } catch (SQLException e) {
                throw new RuntimeException("list tables query failed", e);
            }
        });
    }

    public String[] listDatabases() throws Exception {
        return queryFrontends(conn -> {
            String sql = "SELECT SCHEMA_NAME FROM `information_schema`.`schemata`";
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql);
                 ResultSet resultSet = preparedStatement.executeQuery()) {
                List<String> result = new ArrayList<>();
                while (resultSet.next()) {
                    String schemaName = resultSet.getString(1);
                    if (!"information_schema".equals(schemaName)) {
                        result.add(schemaName);
                    }
                }
                return result.toArray(new String[0]);
            } catch (SQLException e) {
                throw new RuntimeException("list databases query failed", e);
            }
        });
    }

    public boolean databaseExists(String database) throws Exception {
        if (StringUtils.isBlank(database)) {
            return false;
        }
        return queryFrontends(conn -> {
            String sql = "SELECT SCHEMA_NAME FROM `information_schema`.`schemata` WHERE SCHEMA_NAME = '" + database + "'";
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql);
                 ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    if (resultSet.getString(1).equals(database)) {
                        return true;
                    }
                }
                return false;
            } catch (SQLException e) {
                throw new RuntimeException("check databases exists query failed", e);
            }
        });
    }

    public Schema getTableSchema(String db, String table) throws Exception {
        return requestFrontends((frontend, httpClient) -> {
            String url = URLs.tableSchema(frontend.getHost(), frontend.getHttpPort(), db, table, isHttpsEnabled);
            HttpGet httpGet = new HttpGet(url);
            HttpUtils.setAuth(httpGet, username, password);
            Schema dorisSchema;
            try {
                HttpResponse response = httpClient.execute(httpGet);
                JsonNode dataNode = extractDataFromResponse(response, url);
                dorisSchema = MAPPER.readValue(dataNode.traverse(), Schema.class);
            } catch (IOException e) {
                throw new RuntimeException("table schema request failed", e);
            }
            return dorisSchema;
        });
    }

    private List<Frontend> parseFrontends(ArrayNode columnNames, ArrayNode rows) {
        int hostIdx = -1;
        int httpPortIdx = -1;
        int queryPortIdx = -1;
        int flightSqlIdx = -1;
        for (int idx = 0; idx < columnNames.size(); idx++) {
            String columnName = columnNames.get(idx).asText();
            switch (columnName) {
                case "Host":
                case "HostName":
                    hostIdx = idx;
                    break;
                case "HttpPort":
                    httpPortIdx = idx;
                    break;
                case "QueryPort":
                    queryPortIdx = idx;
                    break;
                case "ArrowFlightSqlPort":
                    flightSqlIdx = idx;
                    break;
                default:
                    break;
            }
        }
        if (rows.isEmpty()) {
            return Collections.emptyList();
        }
        List<Frontend> frontends = new ArrayList<>();
        for (int rowIdx = 0; rowIdx < rows.size(); rowIdx++) {
            ArrayNode row = (ArrayNode) rows.get(rowIdx);
            if (flightSqlIdx == -1) {
                frontends.add(new Frontend(row.get(hostIdx).asText(), row.get(httpPortIdx).asInt(), row.get(queryPortIdx).asInt()));
            } else {
                frontends.add(new Frontend(row.get(hostIdx).asText(), row.get(httpPortIdx).asInt(), row.get(queryPortIdx).asInt(), row.get(flightSqlIdx).asInt()));
            }
        }
        return frontends;
    }

    public QueryPlan getQueryPlan(String database, String table, String sql) throws Exception {
        return requestFrontends((frontend, httpClient) -> {
            try {
                String url = URLs.queryPlan(frontend.getHost(), frontend.getHttpPort(), database, table, isHttpsEnabled);
                HttpPost httpPost = new HttpPost(url);
                HttpUtils.setAuth(httpPost, username, password);
                String body = MAPPER.writeValueAsString(ImmutableMap.of("sql", sql));
                httpPost.setEntity(new StringEntity(body));
                HttpResponse response = httpClient.execute(httpPost);
                JsonNode dataJsonNode = extractDataFromResponse(response, url);
                if (dataJsonNode.get("exception") != null) {
                    throw new DorisException("query plan failed, exception: " + dataJsonNode.get("exception").asText());
                }
                return MAPPER.readValue(dataJsonNode.traverse(), QueryPlan.class);
            } catch (Exception e) {
                throw new RuntimeException("query plan request failed", e);
            }
        });
    }


    private JsonNode extractDataFromResponse(HttpResponse response, String url) throws IOException {
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new RuntimeException("request fe with url: [" + url + "] failed with http code: "
                        + response.getStatusLine().getStatusCode() + ", reason: "
                        + response.getStatusLine().getReasonPhrase());
            }
            String entity = EntityUtils.toString(response.getEntity());
            JsonNode respNode = MAPPER.readTree(entity);
            String code = respNode.get("code").asText();
            if (!"0".equalsIgnoreCase(code)) {
                throw new RuntimeException("fetch fe url: [" + url + "]  failed with invalid msg code, response: " + entity);
            }
            return respNode.get("data");
        }

        public String[] getTableAllColumns (String db, String table) throws Exception {
            Schema tableSchema = getTableSchema(db, table);
            return tableSchema.getProperties().stream().map(Field::getName).toArray(String[]::new);
        }

        public List<Backend> getAliveBackends () throws Exception {
            return requestFrontends((frontend, client) -> {
                String url = URLs.aliveBackend(frontend.getHost(), frontend.getHttpPort(), isHttpsEnabled);
                HttpGet httpGet = new HttpGet(url);
                HttpUtils.setAuth(httpGet, username, password);
                ArrayNode backendsNode;
                try {
                    CloseableHttpResponse res = client.execute(httpGet);
                    JsonNode dataNode = extractDataFromResponse(res, url);
                    backendsNode = (ArrayNode) dataNode.get("backends");
                } catch (IOException e) {
                    throw new RuntimeException("get alive backends failed", e);
                }
                List<Backend> backends = new ArrayList<>();
                for (JsonNode backendNode : backendsNode) {
                    if ("true".equalsIgnoreCase(backendNode.get("is_alive").asText())) {
                        backends.add(new Backend(backendNode.get("ip").asText(), backendNode.get("http_port").asInt(), -1));
                    }
                }
                return backends;
            });
        }

        public void truncateTable (String database, String table) throws Exception {
            queryFrontends(conn -> {
                String sql = "TRUNCATE TABLE " + database + "." + table;
                try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                    preparedStatement.execute();
                    return null;
                } catch (SQLException e) {
                    throw new RuntimeException("truncate table failed", e);
                }
            });
        }

        public List<Frontend> getFrontends () {
            return frontends;
        }

        public void close () throws IOException {
            if (httpClient != null) {
                httpClient.close();
            }
        }

    }