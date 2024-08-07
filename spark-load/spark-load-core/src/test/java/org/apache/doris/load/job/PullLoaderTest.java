package org.apache.doris.load.job;

import org.apache.doris.client.DorisClient;
import org.apache.doris.common.enums.TaskType;
import org.apache.doris.common.meta.LoadMeta;
import org.apache.doris.common.meta.TableMeta;
import org.apache.doris.config.EtlJobConfig;
import org.apache.doris.config.JobConfig;
import org.apache.doris.exception.SparkLoadException;
import org.apache.doris.load.LoaderFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.io.FileUtils;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class PullLoaderTest {

    @Test
    void canBeRecovered() throws SparkLoadException, IOException {

        JobConfig jobConfig = new JobConfig();
        jobConfig.setFeAddresses("127.0.0.1:8080");
        Map<String, JobConfig.TaskInfo> loadTasks = new HashMap<>();
        JobConfig.TaskInfo taskInfo = new JobConfig.TaskInfo();
        taskInfo.setType(TaskType.FILE);
        taskInfo.setPaths(Collections.singletonList("test"));
        loadTasks.put("tbl1", taskInfo);
        jobConfig.setLoadTasks(loadTasks);
        jobConfig.setLabel("test");
        File file = new File(System.getProperty("java.io.tmpdir"));
        jobConfig.setWorkingDir(file.getAbsolutePath());

        new MockUp<DorisClient.FeClient>() {
            @Mock
            public LoadMeta createIngestionLoad(String db, Map<String, List<String>> tableToPartition, String label,
                                                Map<String, String> properties) {
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
                try {
                    System.out.println(JsonMapper.builder().build().writeValueAsString(loadMeta));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
                return loadMeta;
            }
        };
        Loader loader = LoaderFactory.createLoader(jobConfig, true);
        assertInstanceOf(Recoverable.class, loader);
        loader.prepare();
        assertFalse(((Recoverable)loader).canBeRecovered());

        File file1 = new File(System.getProperty("java.io.tmpdir") + "/jobs/1/test");
        try {

            file1.mkdirs();
            assertFalse(((Recoverable)loader).canBeRecovered());

            File file2 = new File(System.getProperty("java.io.tmpdir") + "/jobs/1/test/1");
            file2.mkdirs();
            assertFalse(((Recoverable)loader).canBeRecovered());

            File file3 = new File(System.getProperty("java.io.tmpdir") + "/jobs/1/test/1/dpp_result.json");
            Files.write(file3.toPath(), Collections.singletonList(""));
            assertFalse(((Recoverable)loader).canBeRecovered());

            Files.write(file3.toPath(), Collections.singletonList("test"));
            assertThrows(SparkLoadException.class, () -> ((Recoverable)loader).canBeRecovered());

            Files.write(file3.toPath(), Collections.singletonList("{}"));
            assertThrows(SparkLoadException.class, () -> ((Recoverable)loader).canBeRecovered());

            Files.write(file3.toPath(), Collections.singletonList("{\"is_success\":false,\"failed_reason\":\"\"," +
                    "\"scanned_rows\":0,\"file_number\":0,\"file_size\":0,\"normal_rows\":0,\"abnormal_rows\":0," +
                    "\"unselect_rows\":0,\"partial_abnormal_rows\":\"\",\"scanned_bytes\":0}\n"));
            assertFalse(((Recoverable)loader).canBeRecovered());

            Files.write(file3.toPath(), Collections.singletonList("{\"is_success\":true,\"failed_reason\":\"\"," +
                    "\"scanned_rows\":0,\"file_number\":0,\"file_size\":0,\"normal_rows\":0,\"abnormal_rows\":0," +
                    "\"unselect_rows\":0,\"partial_abnormal_rows\":\"\",\"scanned_bytes\":0}\n"));

            File file4 = new File(System.getProperty("java.io.tmpdir") + "/jobs/1/test/1/load_meta.json");
            Files.write(file4.toPath(), Collections.singletonList(""));
            assertFalse(((Recoverable)loader).canBeRecovered());

            Files.write(file4.toPath(), Collections.singletonList("{\"loadId\":1,\"txnId\":1,\"dbId\":1,\"signature\":1," +
                    "\"tableMeta\":{\"tbl1\":{\"id\":1,\"indexes\":[],\"partitionInfo\":{\"partitionType\":" +
                    "\"UNPARTITIONED\",\"partitionColumnRefs\":[],\"distributionColumnRefs\":[],\"partitions\":" +
                    "[{\"partitionId\":1,\"startKeys\":[],\"endKeys\":[],\"isMaxPartition\":true,\"bucketNum\":1}]}" +
                    "}}}\n"));
            assertFalse(((Recoverable)loader).canBeRecovered());

            Files.write(file4.toPath(), Collections.singletonList("{\"loadId\":1,\"txnId\":1,\"dbId\":1,\"signature\":1," +
                    "\"tableMeta\":{\"tbl2\":{\"id\":1,\"indexes\":[{\"indexId\":0,\"columns\":[{\"columnName\":\"c0\"," +
                    "\"columnType\":\"INT\",\"isAllowNull\":true,\"isKey\":true,\"aggregationType\":\"NONE\"," +
                    "\"defaultValue\":\"0\",\"stringLength\":0,\"precision\":0,\"scale\":0,\"defineExpr\":null}]," +
                    "\"schemaHash\":0,\"indexType\":null,\"isBaseIndex\":false,\"schemaVersion\":0}],\"partitionInfo\":" +
                    "{\"partitionType\":\"UNPARTITIONED\",\"partitionColumnRefs\":[],\"distributionColumnRefs\":[]," +
                    "\"partitions\":[{\"partitionId\":1,\"startKeys\":[],\"endKeys\":[],\"isMaxPartition\":true," +
                    "\"bucketNum\":1}]}}}}"));
            assertFalse(((Recoverable)loader).canBeRecovered());

            Files.write(file4.toPath(), Collections.singletonList("{\"loadId\":1,\"txnId\":1,\"dbId\":1,\"signature\":1," +
                    "\"tableMeta\":{\"tbl1\":{\"id\":1,\"indexes\":[{\"indexId\":1,\"columns\":[{\"columnName\":\"c0\"," +
                    "\"columnType\":\"INT\",\"isAllowNull\":true,\"isKey\":true,\"aggregationType\":\"NONE\"," +
                    "\"defaultValue\":\"0\",\"stringLength\":0,\"precision\":0,\"scale\":0,\"defineExpr\":null}]," +
                    "\"schemaHash\":0,\"indexType\":null,\"isBaseIndex\":false,\"schemaVersion\":0}],\"partitionInfo\":" +
                    "{\"partitionType\":\"UNPARTITIONED\",\"partitionColumnRefs\":[],\"distributionColumnRefs\":[]," +
                    "\"partitions\":[{\"partitionId\":1,\"startKeys\":[],\"endKeys\":[],\"isMaxPartition\":true," +
                    "\"bucketNum\":1}]}}}}"));
            assertFalse(((Recoverable)loader).canBeRecovered());

            Files.write(file4.toPath(), Collections.singletonList("{\"loadId\":1,\"txnId\":1,\"dbId\":1,\"signature\":1," +
                    "\"tableMeta\":{\"tbl1\":{\"id\":1,\"indexes\":[{\"indexId\":0,\"columns\":[{\"columnName\":\"c0\"," +
                    "\"columnType\":\"INT\",\"isAllowNull\":true,\"isKey\":true,\"aggregationType\":\"NONE\"," +
                    "\"defaultValue\":\"0\",\"stringLength\":0,\"precision\":0,\"scale\":0,\"defineExpr\":null}]," +
                    "\"schemaHash\":1,\"indexType\":null,\"isBaseIndex\":false,\"schemaVersion\":0}],\"partitionInfo\":" +
                    "{\"partitionType\":\"UNPARTITIONED\",\"partitionColumnRefs\":[],\"distributionColumnRefs\":[]," +
                    "\"partitions\":[{\"partitionId\":1,\"startKeys\":[],\"endKeys\":[],\"isMaxPartition\":true," +
                    "\"bucketNum\":1}]}}}}"));
            assertFalse(((Recoverable)loader).canBeRecovered());

            Files.write(file4.toPath(), Collections.singletonList("{\"loadId\":1,\"txnId\":1,\"dbId\":1,\"signature\":1," +
                    "\"tableMeta\":{\"tbl1\":{\"id\":1,\"indexes\":[{\"indexId\":0,\"columns\":[{\"columnName\":\"c0\"," +
                    "\"columnType\":\"INT\",\"isAllowNull\":true,\"isKey\":true,\"aggregationType\":\"NONE\"," +
                    "\"defaultValue\":\"0\",\"stringLength\":0,\"precision\":0,\"scale\":0,\"defineExpr\":null}]," +
                    "\"schemaHash\":0,\"indexType\":null,\"isBaseIndex\":false,\"schemaVersion\":1}],\"partitionInfo\":" +
                    "{\"partitionType\":\"UNPARTITIONED\",\"partitionColumnRefs\":[],\"distributionColumnRefs\":[]," +
                    "\"partitions\":[{\"partitionId\":1,\"startKeys\":[],\"endKeys\":[],\"isMaxPartition\":true," +
                    "\"bucketNum\":1}]}}}}"));
            assertFalse(((Recoverable)loader).canBeRecovered());

            Files.write(file4.toPath(), Collections.singletonList("{\"loadId\":1,\"txnId\":1,\"dbId\":1,\"signature\":1," +
                    "\"tableMeta\":{\"tbl1\":{\"id\":1,\"indexes\":[{\"indexId\":0,\"columns\":[{\"columnName\":\"c0\"," +
                    "\"columnType\":\"INT\",\"isAllowNull\":true,\"isKey\":true,\"aggregationType\":\"NONE\"," +
                    "\"defaultValue\":\"0\",\"stringLength\":0,\"precision\":0,\"scale\":0,\"defineExpr\":null}]," +
                    "\"schemaHash\":0,\"indexType\":null,\"isBaseIndex\":false,\"schemaVersion\":0}],\"partitionInfo\":" +
                    "{\"partitionType\":\"UNPARTITIONED\",\"partitionColumnRefs\":[],\"distributionColumnRefs\":[]," +
                    "\"partitions\":[{\"partitionId\":1,\"startKeys\":[],\"endKeys\":[],\"isMaxPartition\":true," +
                    "\"bucketNum\":1},{\"partitionId\":2,\"startKeys\":[],\"endKeys\":[],\"isMaxPartition\":true," +
                    "\"bucketNum\":1}]}}}}"));
            assertFalse(((Recoverable)loader).canBeRecovered());

            Files.write(file4.toPath(), Collections.singletonList("{\"loadId\":1,\"txnId\":1,\"dbId\":1,\"signature\":1," +
                    "\"tableMeta\":{\"tbl1\":{\"id\":1,\"indexes\":[{\"indexId\":0,\"columns\":[{\"columnName\":\"c0\"," +
                    "\"columnType\":\"INT\",\"isAllowNull\":true,\"isKey\":true,\"aggregationType\":\"NONE\"," +
                    "\"defaultValue\":\"0\",\"stringLength\":0,\"precision\":0,\"scale\":0,\"defineExpr\":null}]," +
                    "\"schemaHash\":0,\"indexType\":null,\"isBaseIndex\":false,\"schemaVersion\":0}],\"partitionInfo\":" +
                    "{\"partitionType\":\"UNPARTITIONED\",\"partitionColumnRefs\":[],\"distributionColumnRefs\":[]," +
                    "\"partitions\":[{\"partitionId\":2,\"startKeys\":[],\"endKeys\":[],\"isMaxPartition\":true," +
                    "\"bucketNum\":1}]}}}}"));
            assertFalse(((Recoverable)loader).canBeRecovered());

            Files.write(file4.toPath(), Collections.singletonList("{\"loadId\":1,\"txnId\":1,\"dbId\":1,\"signature\":1," +
                    "\"tableMeta\":{\"tbl1\":{\"id\":1,\"indexes\":[{\"indexId\":0,\"columns\":[{\"columnName\":\"c0\"," +
                    "\"columnType\":\"INT\",\"isAllowNull\":true,\"isKey\":true,\"aggregationType\":\"NONE\"," +
                    "\"defaultValue\":\"0\",\"stringLength\":0,\"precision\":0,\"scale\":0,\"defineExpr\":null}]," +
                    "\"schemaHash\":0,\"indexType\":null,\"isBaseIndex\":false,\"schemaVersion\":0}],\"partitionInfo\":" +
                    "{\"partitionType\":\"UNPARTITIONED\",\"partitionColumnRefs\":[],\"distributionColumnRefs\":[]," +
                    "\"partitions\":[{\"partitionId\":1,\"startKeys\":[],\"endKeys\":[],\"isMaxPartition\":true," +
                    "\"bucketNum\":1}]}}}}"));
            assertTrue(((Recoverable)loader).canBeRecovered());

        } finally {
            // delete ${java.io.tmpdir}/jobs on exit
            FileUtils.deleteDirectory(file1.getParentFile().getParentFile());
        }

    }
}