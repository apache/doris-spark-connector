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

package org.apache.doris.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class EtlJobConfigTest {

    @Test
    void getOutputPath() {
        String outputPath = EtlJobConfig.getOutputPath("hdfs://127.0.0.1/spark-load", 10001L, "test", 123L);
        Assertions.assertEquals("hdfs://127.0.0.1/spark-load/jobs/10001/test/123", outputPath);
    }

    @Test
    void getOutputFilePattern() {
        String outputFilePattern = EtlJobConfig.getOutputFilePattern("test", EtlJobConfig.FilePatternVersion.V1);
        Assertions.assertEquals("V1.test.%d.%d.%d.%d.%d.parquet", outputFilePattern);
    }

    @Test
    void configFromJson() {
        List<EtlJobConfig.EtlIndex> etlIndexes = new ArrayList<>();
        List<EtlJobConfig.EtlColumn> etlColumns = new ArrayList<>();
        EtlJobConfig.EtlColumn etlColumn0 = new EtlJobConfig.EtlColumn("c0", "INT", false, true, "NONE", "0", 0, 0, 0);
        EtlJobConfig.EtlColumn etlColumn1 =
                new EtlJobConfig.EtlColumn("c1", "VARCHAR", true, false, "NONE", "\\N", 10, 0, 0);
        etlColumns.add(etlColumn0);
        etlColumns.add(etlColumn1);
        EtlJobConfig.EtlIndex etlIndex = new EtlJobConfig.EtlIndex(1L, etlColumns, 123, "DUPLICATE", true, 0);
        etlIndexes.add(etlIndex);
        EtlJobConfig.EtlPartitionInfo etlPartitionInfo =
                new EtlJobConfig.EtlPartitionInfo("UNPARTITIONED", Collections.emptyList(),
                        Collections.singletonList("c0"), Collections.singletonList(
                        new EtlJobConfig.EtlPartition(0, Collections.emptyList(), Collections.emptyList(), true, 0)));
        EtlJobConfig.EtlTable table = new EtlJobConfig.EtlTable(etlIndexes, etlPartitionInfo);
        Map<Long, EtlJobConfig.EtlTable> tables = new HashMap<>();
        tables.put(123L, table);
        String outputFilePattern = EtlJobConfig.getOutputFilePattern("test", EtlJobConfig.FilePatternVersion.V1);
        EtlJobConfig.EtlJobProperty properties = new EtlJobConfig.EtlJobProperty();
        EtlJobConfig jobConfig = new EtlJobConfig(tables, outputFilePattern, "test", properties);
        Assertions.assertEquals(jobConfig.configToJson(),
                EtlJobConfig.configFromJson("{\"tables\":{\"123\":{\"indexes\":[{\"indexId\":1,\"columns\":[{\"columnName\":\"c0\"," +
                        "\"columnType\":\"INT\",\"isAllowNull\":false,\"isKey\":true,\"aggregationType\":\"NONE\"," +
                        "\"defaultValue\":\"0\",\"stringLength\":0,\"precision\":0,\"scale\":0}," +
                        "{\"columnName\":\"c1\",\"columnType\":\"VARCHAR\",\"isAllowNull\":true,\"isKey\":false," +
                        "\"aggregationType\":\"NONE\",\"defaultValue\":\"\\\\N\",\"stringLength\":10,\"precision\":0," +
                        "\"scale\":0}],\"schemaHash\":123,\"indexType\":\"DUPLICATE\",\"isBaseIndex\":true," +
                        "\"schemaVersion\":0}],\"partitionInfo\":{\"partitionType\":\"UNPARTITIONED\"," +
                        "\"partitionColumnRefs\":[],\"distributionColumnRefs\":[\"c0\"],\"partitions\":" +
                        "[{\"partitionId\":0,\"startKeys\":[],\"endKeys\":[],\"isMaxPartition\":true,\"bucketNum\":0}]}," +
                        "\"fileGroups\":[]}},\"outputFilePattern\":\"V1.test.%d.%d.%d.%d.%d.parquet\"," +
                        "\"label\":\"test\",\"properties\":{\"strictMode\":false},\"configVersion\":\"V1\"}").configToJson());
    }

    @Test
    void configToJson() {
        List<EtlJobConfig.EtlIndex> etlIndexes = new ArrayList<>();
        List<EtlJobConfig.EtlColumn> etlColumns = new ArrayList<>();
        EtlJobConfig.EtlColumn etlColumn0 = new EtlJobConfig.EtlColumn("c0", "INT", false, true, "NONE", "0", 0, 0, 0);
        EtlJobConfig.EtlColumn etlColumn1 =
                new EtlJobConfig.EtlColumn("c1", "VARCHAR", true, false, "NONE", "\\N", 10, 0, 0);
        etlColumns.add(etlColumn0);
        etlColumns.add(etlColumn1);
        EtlJobConfig.EtlIndex etlIndex = new EtlJobConfig.EtlIndex(1L, etlColumns, 123, "DUPLICATE", true, 0);
        etlIndexes.add(etlIndex);
        EtlJobConfig.EtlPartitionInfo etlPartitionInfo =
                new EtlJobConfig.EtlPartitionInfo("UNPARTITIONED", Collections.emptyList(),
                        Collections.singletonList("c0"), Collections.singletonList(
                        new EtlJobConfig.EtlPartition(0, Collections.emptyList(), Collections.emptyList(), true, 0)));
        EtlJobConfig.EtlTable table = new EtlJobConfig.EtlTable(etlIndexes, etlPartitionInfo);
        Map<Long, EtlJobConfig.EtlTable> tables = new HashMap<>();
        tables.put(123L, table);
        String outputFilePattern = EtlJobConfig.getOutputFilePattern("test", EtlJobConfig.FilePatternVersion.V1);
        EtlJobConfig.EtlJobProperty properties = new EtlJobConfig.EtlJobProperty();
        EtlJobConfig jobConfig = new EtlJobConfig(tables, outputFilePattern, "test", properties);
        Assertions.assertEquals(
                "{\"tables\":{\"123\":{\"indexes\":[{\"indexId\":1,\"columns\":[{\"columnName\":\"c0\"," +
                        "\"columnType\":\"INT\",\"isAllowNull\":false,\"isKey\":true,\"aggregationType\":\"NONE\"," +
                        "\"defaultValue\":\"0\",\"stringLength\":0,\"precision\":0,\"scale\":0}," +
                        "{\"columnName\":\"c1\",\"columnType\":\"VARCHAR\",\"isAllowNull\":true,\"isKey\":false," +
                        "\"aggregationType\":\"NONE\",\"defaultValue\":\"\\\\N\",\"stringLength\":10,\"precision\":0," +
                        "\"scale\":0}],\"schemaHash\":123,\"indexType\":\"DUPLICATE\",\"isBaseIndex\":true," +
                        "\"schemaVersion\":0}],\"partitionInfo\":{\"partitionType\":\"UNPARTITIONED\"," +
                        "\"partitionColumnRefs\":[],\"distributionColumnRefs\":[\"c0\"],\"partitions\":" +
                        "[{\"partitionId\":0,\"startKeys\":[],\"endKeys\":[],\"isMaxPartition\":true,\"bucketNum\":0}]}," +
                        "\"fileGroups\":[]}},\"outputFilePattern\":\"V1.test.%d.%d.%d.%d.%d.parquet\"," +
                        "\"label\":\"test\",\"properties\":{\"strictMode\":false},\"configVersion\":\"V1\"}",
                jobConfig.configToJson());
    }
}