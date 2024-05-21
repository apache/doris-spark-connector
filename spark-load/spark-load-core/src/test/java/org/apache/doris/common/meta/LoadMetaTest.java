package org.apache.doris.common.meta;


import org.apache.doris.exception.SparkLoadException;
import org.apache.doris.sparkdpp.EtlJobConfig;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LoadMetaTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void checkMapping() throws SparkLoadException {

        List<EtlJobConfig.EtlColumn> columns = new ArrayList<>();
        columns.add(new EtlJobConfig.EtlColumn("id", "BIGINT", false, true, "NONE", null, 0, 10, 0));
        columns.add(new EtlJobConfig.EtlColumn("c1", "HLL", true, false, "NONE", null, 0, 10, 0));
        columns.add(new EtlJobConfig.EtlColumn("c2", "BITMAP", true, false, "NONE", null, 0, 10, 0));

        EtlJobConfig.EtlIndex etlIndex = new EtlJobConfig.EtlIndex(1, columns, 1, "DUPLICATE", true);
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
        Assert.assertThrows(SparkLoadException.class, () -> loadMeta.checkMapping(etlTable, columnMappingMap));

        Map<String, EtlJobConfig.EtlColumnMapping> columnMappingMap1 = new HashMap<>();
        columnMappingMap1.put("c1", new EtlJobConfig.EtlColumnMapping("hll_hash(c1)"));
        Assert.assertThrows(SparkLoadException.class, () -> loadMeta.checkMapping(etlTable, columnMappingMap1));

        Map<String, EtlJobConfig.EtlColumnMapping> columnMappingMap2 = new HashMap<>();
        columnMappingMap2.put("c1", new EtlJobConfig.EtlColumnMapping("hll_hash(c1)"));
        columnMappingMap2.put("c2", new EtlJobConfig.EtlColumnMapping("to_bitmap(c1)"));
        loadMeta.checkMapping(etlTable, columnMappingMap2);

    }
}