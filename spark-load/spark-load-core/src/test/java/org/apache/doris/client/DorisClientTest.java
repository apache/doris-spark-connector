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

import org.apache.doris.common.meta.LoadMeta;
import org.apache.doris.common.meta.TableMeta;
import org.apache.doris.config.EtlJobConfig;
import org.apache.doris.exception.SparkLoadException;
import org.apache.doris.util.JsonUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import mockit.Mock;
import mockit.MockUp;
import org.apache.http.Header;
import org.apache.http.HeaderIterator;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.params.HttpParams;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

class DorisClientTest {

    @Test
    public void getFeClient() {
        IllegalArgumentException e1 =
                Assertions.assertThrows(IllegalArgumentException.class, () -> DorisClient.getFeClient("", "", ""));
        Assertions.assertEquals("feAddresses is empty", e1.getMessage());
        IllegalArgumentException e2 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> DorisClient.getFeClient("127.0.0.1", "", ""));
        Assertions.assertEquals("feAddresses contains invalid format, 127.0.0.1", e2.getMessage());
        IllegalArgumentException e3 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> DorisClient.getFeClient("127.0.0.1:", "", ""));
        Assertions.assertEquals("feAddresses contains invalid format, 127.0.0.1:", e3.getMessage());
        IllegalArgumentException e4 =
                Assertions.assertThrows(IllegalArgumentException.class, () -> DorisClient.getFeClient(":8030", "", ""));
        Assertions.assertEquals("feAddresses contains invalid format, :8030", e4.getMessage());
        Assertions.assertDoesNotThrow(() -> DorisClient.getFeClient("127.0.0.1:8030", "", ""));
    }

    @Test
    public void createIngestionLoad() throws SparkLoadException, JsonProcessingException {

        DorisClient.FeClient feClient = new DorisClient.FeClient("127.0.0.1:8030", "", "");

        new MockUp<CloseableHttpClient>(CloseableHttpClient.class) {
            @Mock
            public CloseableHttpResponse execute(
                    final HttpUriRequest request) throws IOException, ClientProtocolException {
                MockedCloseableHttpResponse response = new MockedCloseableHttpResponse();
                response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
                return response;
            }
        };
        Assertions.assertThrows(SparkLoadException.class, () -> feClient.createIngestionLoad("db", new HashMap<>(), "test", new HashMap<>()));

        new MockUp<CloseableHttpClient>(CloseableHttpClient.class) {
            @Mock
            public CloseableHttpResponse execute(
                    final HttpUriRequest request) throws IOException, ClientProtocolException {
                MockedCloseableHttpResponse response = new MockedCloseableHttpResponse();
                response.setStatusCode(HttpStatus.SC_OK);
                response.setEntity(new StringEntity("{\"code\":1,\"msg\":\"\",\"data\":{},\"count\":0}"));
                return response;
            }
        };
        Assertions.assertThrows(SparkLoadException.class, () -> feClient.createIngestionLoad("db", new HashMap<>(), "test", new HashMap<>()));

        new MockUp<CloseableHttpClient>(CloseableHttpClient.class) {
            @Mock
            public CloseableHttpResponse execute(
                    final HttpUriRequest request) throws IOException, ClientProtocolException {
                MockedCloseableHttpResponse response = new MockedCloseableHttpResponse();
                response.setStatusCode(HttpStatus.SC_OK);
                response.setEntity(new StringEntity("{\"code\":0,\"msg\":\"\",\"data\":{\"loadId\":1,\"txnId\":1," +
                        "\"dbId\":1,\"signature\":1,\"tableMeta\":{\"tbl1\":{\"id\":1," +
                        "\"indexes\":[{\"indexId\":0,\"columns\":[{\"columnName\":\"c0\",\"columnType\":\"INT\"," +
                        "\"isAllowNull\":true,\"isKey\":true,\"aggregationType\":\"NONE\",\"defaultValue\":\"0\"," +
                        "\"stringLength\":0,\"precision\":0,\"scale\":0,\"defineExpr\":null}],\"schemaHash\":0," +
                        "\"indexType\":null,\"isBaseIndex\":false,\"schemaVersion\":0}],\"partitionInfo\":" +
                        "{\"partitionType\":\"UNPARTITIONED\",\"partitionColumnRefs\":[],\"distributionColumnRefs\":[]," +
                        "\"partitions\":[{\"partitionId\":1,\"startKeys\":[],\"endKeys\":[],\"isMaxPartition\":true," +
                        "\"bucketNum\":1}]}}}},\"count\":0}"));
                return response;
            }
        };

        LoadMeta loadMeta = new LoadMeta();
        loadMeta.setLoadId(1L);
        loadMeta.setTxnId(1L);
        loadMeta.setDbId(1L);
        loadMeta.setSignature(1L);
        Map<String, TableMeta> tableMetaMap = new HashMap<>();
        TableMeta tableMeta = new TableMeta();
        tableMeta.setId(1L);
        List<TableMeta.EtlIndex> indexList = new ArrayList<>();
        TableMeta.EtlIndex index = new TableMeta.EtlIndex();
        List<EtlJobConfig.EtlColumn> columnList = new ArrayList<>();
        EtlJobConfig.EtlColumn column = new EtlJobConfig.EtlColumn();
        column.columnName = "c0";
        column.columnType = "INT";
        column.defaultValue = "0";
        column.isAllowNull = true;
        column.aggregationType = "NONE";
        column.isKey = true;
        columnList.add(column);
        index.columns = columnList;
        indexList.add(index);
        tableMeta.setIndexes(indexList);
        TableMeta.EtlPartitionInfo partitionInfo = new TableMeta.EtlPartitionInfo();
        TableMeta.EtlPartition partition = new TableMeta.EtlPartition();
        partition.partitionId = 1;
        partition.bucketNum = 1;
        partition.startKeys = Collections.emptyList();
        partition.endKeys = Collections.emptyList();
        partition.isMaxPartition = true;
        partitionInfo.partitions = Collections.singletonList(partition);
        partitionInfo.partitionType = "UNPARTITIONED";
        partitionInfo.partitionColumnRefs = new ArrayList<>();
        partitionInfo.distributionColumnRefs = new ArrayList<>();
        tableMeta.setPartitionInfo(partitionInfo);
        tableMetaMap.put("tbl1", tableMeta);
        loadMeta.setTableMeta(tableMetaMap);
        Assertions.assertEquals(JsonUtils.writeValueAsString(loadMeta),
                JsonUtils.writeValueAsString(feClient.createIngestionLoad("db", new HashMap<>(), "test", new HashMap<>())));

    }

    @Test
    public void updateIngestionLoad() {

        DorisClient.FeClient feClient = new DorisClient.FeClient("127.0.0.1:8030", "", "");

        new MockUp<CloseableHttpClient>(CloseableHttpClient.class) {
            @Mock
            public CloseableHttpResponse execute(
                    final HttpUriRequest request) throws IOException, ClientProtocolException {
                MockedCloseableHttpResponse response = new MockedCloseableHttpResponse();
                response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
                return response;
            }
        };
        Assertions.assertThrows(SparkLoadException.class, () -> feClient.updateIngestionLoad("db", 1L, new HashMap<>()));

        new MockUp<CloseableHttpClient>(CloseableHttpClient.class) {
            @Mock
            public CloseableHttpResponse execute(
                    final HttpUriRequest request) throws IOException, ClientProtocolException {
                MockedCloseableHttpResponse response = new MockedCloseableHttpResponse();
                response.setStatusCode(HttpStatus.SC_OK);
                response.setEntity(new StringEntity("{\"code\":1,\"msg\":\"\",\"data\":{},\"count\":0}"));
                return response;
            }
        };
        Assertions.assertThrows(SparkLoadException.class, () -> feClient.updateIngestionLoad("db", 1L, new HashMap<>()));

        new MockUp<CloseableHttpClient>(CloseableHttpClient.class) {
            @Mock
            public CloseableHttpResponse execute(
                    final HttpUriRequest request) throws IOException, ClientProtocolException {
                MockedCloseableHttpResponse response = new MockedCloseableHttpResponse();
                response.setStatusCode(HttpStatus.SC_OK);
                response.setEntity(new StringEntity("{\"code\":0,\"msg\":\"\",\"data\":{},\"count\":0}"));
                return response;
            }
        };
        Assertions.assertDoesNotThrow(() -> feClient.updateIngestionLoad("db", 1L, new HashMap<>()));

    }

    @Test
    public void getLoadInfo() throws SparkLoadException, JsonProcessingException {

        DorisClient.FeClient feClient = new DorisClient.FeClient("127.0.0.1:8030", "", "");

        new MockUp<CloseableHttpClient>(CloseableHttpClient.class) {
            @Mock
            public CloseableHttpResponse execute(
                    final HttpUriRequest request) throws IOException, ClientProtocolException {
                MockedCloseableHttpResponse response = new MockedCloseableHttpResponse();
                response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
                return response;
            }
        };
        Assertions.assertThrows(SparkLoadException.class, () -> feClient.getLoadInfo("db", "test"));

        new MockUp<CloseableHttpClient>(CloseableHttpClient.class) {
            @Mock
            public CloseableHttpResponse execute(
                    final HttpUriRequest request) throws IOException, ClientProtocolException {
                MockedCloseableHttpResponse response = new MockedCloseableHttpResponse();
                response.setStatusCode(HttpStatus.SC_OK);
                response.setEntity(new StringEntity("{\"status\":\"err\",\"msg\":\"\",\"jobInfo\":{\"dbName\":\"db\"," +
                        "\"tblNames\":[\"tbl1\"],\"label\":\"test\",\"clusterName\":\"default\",\"state\":\"FINISHED\"," +
                        "\"failMsg\":\"\",\"trackingUrl\":\"\"}}"));
                return response;
            }
        };
        Assertions.assertThrows(SparkLoadException.class, () -> feClient.getLoadInfo("db", "test"));

        new MockUp<CloseableHttpClient>(CloseableHttpClient.class) {
            @Mock
            public CloseableHttpResponse execute(
                    final HttpUriRequest request) throws IOException, ClientProtocolException {
                MockedCloseableHttpResponse response = new MockedCloseableHttpResponse();
                response.setStatusCode(HttpStatus.SC_OK);
                response.setEntity(new StringEntity("{\"status\":\"ok\",\"msg\":\"\",\"jobInfo\":{\"dbName\":\"db\"," +
                        "\"tblNames\":[\"tbl1\"],\"label\":\"test\",\"clusterName\":\"default\",\"state\":\"FINISHED\"," +
                        "\"failMsg\":\"\",\"trackingUrl\":\"\"}}"));
                return response;
            }
        };
        Assertions.assertEquals("{\"dbName\":\"db\",\"tblNames\":[\"tbl1\"],\"label\":\"test\"," +
                "\"clusterName\":\"default\",\"state\":\"FINISHED\",\"failMsg\":\"\",\"trackingUrl\":\"\"}",
                JsonUtils.writeValueAsString(feClient.getLoadInfo("db", "test")));

    }

    @Test
    public void getDDL() {

        DorisClient.FeClient feClient = new DorisClient.FeClient("127.0.0.1:8030", "", "");

        new MockUp<CloseableHttpClient>(CloseableHttpClient.class) {
            @Mock
            public CloseableHttpResponse execute(
                    final HttpUriRequest request) throws IOException, ClientProtocolException {
                MockedCloseableHttpResponse response = new MockedCloseableHttpResponse();
                response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
                return response;
            }
        };
        SparkLoadException e1 =
                Assertions.assertThrows(SparkLoadException.class, () -> feClient.getDDL("db", "test"));
        Assertions.assertEquals("request get ddl failed, path: /api/_get_ddl", e1.getMessage());

        new MockUp<CloseableHttpClient>(CloseableHttpClient.class) {
            @Mock
            public CloseableHttpResponse execute(
                    final HttpUriRequest request) throws IOException, ClientProtocolException {
                MockedCloseableHttpResponse response = new MockedCloseableHttpResponse();
                response.setStatusCode(HttpStatus.SC_OK);
                response.setEntity(new StringEntity("{\"code\":1,\"msg\":\"\",\"data\":{},\"count\":0}"));
                return response;
            }
        };
        SparkLoadException e2 =
                Assertions.assertThrows(SparkLoadException.class, () -> feClient.getDDL("db", "test"));
        Assertions.assertEquals("get ddl failed, status: 1, msg: , data: {}", e2.getMessage());

        new MockUp<CloseableHttpClient>(CloseableHttpClient.class) {
            @Mock
            public CloseableHttpResponse execute(
                    final HttpUriRequest request) throws IOException, ClientProtocolException {
                MockedCloseableHttpResponse response = new MockedCloseableHttpResponse();
                response.setStatusCode(HttpStatus.SC_OK);
                response.setEntity(new StringEntity("{\"code\":0,\"msg\":\"\",\"data\":{},\"count\":0}"));
                return response;
            }
        };
        SparkLoadException e3 =
                Assertions.assertThrows(SparkLoadException.class, () -> feClient.getDDL("db", "test"));
        Assertions.assertEquals("get ddl failed, status: 0, msg: , data: {}", e3.getMessage());

        new MockUp<CloseableHttpClient>(CloseableHttpClient.class) {
            @Mock
            public CloseableHttpResponse execute(
                    final HttpUriRequest request) throws IOException, ClientProtocolException {
                MockedCloseableHttpResponse response = new MockedCloseableHttpResponse();
                response.setStatusCode(HttpStatus.SC_OK);
                response.setEntity(new StringEntity("{\"code\":0,\"msg\":\"\"," +
                        "\"data\":{\"create_table\": [\"CREATE TABLE `tbl1` (\\n  `k1` int(11) NULL " +
                        "COMMENT \\\"\\\",\\n  `k2` int(11) NULL COMMENT \\\"\\\"\\n) ENGINE=OLAP\\n" +
                        "DUPLICATE KEY(`k1`, `k2`)\\nCOMMENT \\\"OLAP\\\"\\nDISTRIBUTED BY HASH(`k1`) BUCKETS 1\\n" +
                        "PROPERTIES (\\n\\\"replication_num\\\" = \\\"1\\\",\\n\\\"version_info\\\" = \\\"1,0\\\",\\n" +
                        "\\\"in_memory\\\" = \\\"false\\\",\\n\\\"storage_format\\\" = \\\"DEFAULT\\\"\\n);\"]\n}," +
                        "\"count\":0}"));
                return response;
            }
        };
        Assertions.assertDoesNotThrow(() -> feClient.getDDL("db", "test"));


    }

    private class MockedCloseableHttpResponse implements CloseableHttpResponse {

        private StatusLine statusLine;
        private HttpEntity entity;

        @Override
        public void close() throws IOException {

        }

        @Override
        public StatusLine getStatusLine() {
            return statusLine;
        }

        @Override
        public void setStatusLine(StatusLine statusline) {
            this.statusLine = statusline;
        }

        @Override
        public void setStatusLine(ProtocolVersion ver, int code) {
            this.statusLine = new BasicStatusLine(ver, code, "");
        }

        @Override
        public void setStatusLine(ProtocolVersion ver, int code, String reason) {
            this.statusLine = new BasicStatusLine(ver, code, reason);
        }

        @Override
        public void setStatusCode(int code) throws IllegalStateException {
            if (this.statusLine == null) {
                this.statusLine = new BasicStatusLine(HttpVersion.HTTP_1_1, code, "");
            } else {
                this.statusLine = new BasicStatusLine(statusLine.getProtocolVersion(), code, statusLine.getReasonPhrase());
            }
        }

        @Override
        public void setReasonPhrase(String reason) throws IllegalStateException {
            if (this.statusLine == null) {
                this.statusLine = new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, reason);
            } else {
                this.statusLine = new BasicStatusLine(statusLine.getProtocolVersion(), statusLine.getStatusCode(), reason);
            }
        }

        @Override
        public HttpEntity getEntity() {
            return entity;
        }

        @Override
        public void setEntity(HttpEntity entity) {
            this.entity = entity;
        }

        @Override
        public Locale getLocale() {
            return null;
        }

        @Override
        public void setLocale(Locale loc) {

        }

        @Override
        public ProtocolVersion getProtocolVersion() {
            return HttpVersion.HTTP_1_1;
        }

        @Override
        public boolean containsHeader(String name) {
            return false;
        }

        @Override
        public Header[] getHeaders(String name) {
            return new Header[0];
        }

        @Override
        public Header getFirstHeader(String name) {
            return null;
        }

        @Override
        public Header getLastHeader(String name) {
            return null;
        }

        @Override
        public Header[] getAllHeaders() {
            return new Header[0];
        }

        @Override
        public void addHeader(Header header) {

        }

        @Override
        public void addHeader(String name, String value) {

        }

        @Override
        public void setHeader(Header header) {

        }

        @Override
        public void setHeader(String name, String value) {

        }

        @Override
        public void setHeaders(Header[] headers) {

        }

        @Override
        public void removeHeader(Header header) {

        }

        @Override
        public void removeHeaders(String name) {

        }

        @Override
        public HeaderIterator headerIterator() {
            return null;
        }

        @Override
        public HeaderIterator headerIterator(String name) {
            return null;
        }

        @Override
        public HttpParams getParams() {
            return null;
        }

        @Override
        public void setParams(HttpParams params) {

        }
    }


}