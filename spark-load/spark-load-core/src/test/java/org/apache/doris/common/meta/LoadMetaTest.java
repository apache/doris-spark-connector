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

package org.apache.doris.common.meta;


import org.apache.doris.config.EtlJobConfig;
import org.apache.doris.config.JobConfig;
import org.apache.doris.exception.SparkLoadException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LoadMetaTest {

    @Test
    public void checkMapping() throws SparkLoadException {

        List<EtlJobConfig.EtlColumn> columns = new ArrayList<>();
        columns.add(new EtlJobConfig.EtlColumn("id", "BIGINT", false, true, "NONE", null, 0, 10, 0));
        columns.add(new EtlJobConfig.EtlColumn("c1", "HLL", true, false, "NONE", null, 0, 10, 0));
        columns.add(new EtlJobConfig.EtlColumn("c2", "BITMAP", true, false, "NONE", null, 0, 10, 0));

        EtlJobConfig.EtlIndex etlIndex = new EtlJobConfig.EtlIndex(1, columns, 1, "DUPLICATE", true, 1);
        EtlJobConfig.EtlPartition etlPartition =
                new EtlJobConfig.EtlPartition(1L, Collections.singletonList(0), Collections.singletonList(1), true, 1);
        EtlJobConfig.EtlPartitionInfo etlPartitionInfo =
                new EtlJobConfig.EtlPartitionInfo("RANGE", Collections.singletonList("id"),
                        Collections.singletonList("id"), Collections.singletonList(etlPartition));

        EtlJobConfig.EtlTable etlTable = new EtlJobConfig.EtlTable(Collections.singletonList(etlIndex),
                etlPartitionInfo);

        LoadMeta loadMeta = new LoadMeta();

        Map<String, EtlJobConfig.EtlColumnMapping> columnMappingMap = new HashMap<>();
        columnMappingMap.put("c2", new EtlJobConfig.EtlColumnMapping("to_bitmap(c1)"));
        Assertions.assertThrows(SparkLoadException.class, () -> loadMeta.checkMapping(etlTable, columnMappingMap));

        Map<String, EtlJobConfig.EtlColumnMapping> columnMappingMap1 = new HashMap<>();
        columnMappingMap1.put("c1", new EtlJobConfig.EtlColumnMapping("hll_hash(c1)"));
        Assertions.assertThrows(SparkLoadException.class, () -> loadMeta.checkMapping(etlTable, columnMappingMap1));

        Map<String, EtlJobConfig.EtlColumnMapping> columnMappingMap2 = new HashMap<>();
        columnMappingMap2.put("c1", new EtlJobConfig.EtlColumnMapping("hll_hash(c1)"));
        columnMappingMap2.put("c2", new EtlJobConfig.EtlColumnMapping("to_bitmap(c1)"));
        loadMeta.checkMapping(etlTable, columnMappingMap2);

    }

}