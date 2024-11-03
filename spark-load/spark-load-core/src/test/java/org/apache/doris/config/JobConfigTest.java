package org.apache.doris.config;


import org.apache.doris.client.DorisClient;
import org.apache.doris.common.enums.TaskType;
import org.apache.doris.exception.SparkLoadException;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class JobConfigTest {

    @Test
    public void checkFeAddress() {

        JobConfig jobConfig = new JobConfig();
        jobConfig.setFeAddresses("");
        IllegalArgumentException e1 =
                Assertions.assertThrows(IllegalArgumentException.class, jobConfig::checkFeAddress);
        Assertions.assertEquals("feAddress is empty", e1.getMessage());

        jobConfig.setFeAddresses("127.0.0.1");
        IllegalArgumentException e2 =
                Assertions.assertThrows(IllegalArgumentException.class, jobConfig::checkFeAddress,
                        "feAddress format is incorrect");
        Assertions.assertEquals("feAddress format is incorrect", e2.getMessage());

        jobConfig.setFeAddresses("127.0.0.1,127.0.0.2");
        IllegalArgumentException e3 =
                Assertions.assertThrows(IllegalArgumentException.class, jobConfig::checkFeAddress,
                        "feAddress format is incorrect");
        Assertions.assertEquals("feAddress format is incorrect", e3.getMessage());

        jobConfig.setFeAddresses("127.0.0.1:8030");
        Assertions.assertDoesNotThrow(jobConfig::checkFeAddress);

    }

    @Test
    public void checkTaskInfo() {

        JobConfig jobConfig = new JobConfig();
        jobConfig.setFeAddresses("127.0.0.1:8030");

        jobConfig.setLoadTasks(new HashMap<>());
        IllegalArgumentException e1 =
                Assertions.assertThrows(IllegalArgumentException.class, jobConfig::checkTaskInfo);
        Assertions.assertEquals("loadTasks is empty", e1.getMessage());

        new MockUp<DorisClient.FeClient>(DorisClient.FeClient.class) {
            @Mock
            public String getDDL(String db, String table) throws SparkLoadException {
                return "create table tbl1 (col1 int, col2 int, col3 int, col4 int) unique key (col1) properties (" +
                        "\"enable_unique_key_merge_on_write\" = \"false\")";
            }
        };

        Map<String, JobConfig.TaskInfo> loadTasks1 = new HashMap<>();
        JobConfig.TaskInfo taskInfo1 = new JobConfig.TaskInfo();
        taskInfo1.setType(TaskType.FILE);
        loadTasks1.put("task1", taskInfo1);
        jobConfig.setLoadTasks(loadTasks1);
        IllegalArgumentException e2 =
                Assertions.assertThrows(IllegalArgumentException.class, jobConfig::checkTaskInfo);
        Assertions.assertEquals("file path is empty", e2.getMessage());

        Map<String, JobConfig.TaskInfo> loadTasks2 = new HashMap<>();
        JobConfig.TaskInfo taskInfo2 = new JobConfig.TaskInfo();
        taskInfo2.setType(TaskType.FILE);
        taskInfo2.setPaths(Collections.singletonList("test"));
        taskInfo2.setFormat("sequence");
        loadTasks2.put("task2", taskInfo2);
        jobConfig.setLoadTasks(loadTasks2);
        IllegalArgumentException e3 =
                Assertions.assertThrows(IllegalArgumentException.class, jobConfig::checkTaskInfo);
        Assertions.assertEquals("format only support parquet or orc or csv", e3.getMessage());

        taskInfo2.setFormat("csv");
        Assertions.assertDoesNotThrow(jobConfig::checkTaskInfo);

        Map<String, JobConfig.TaskInfo> loadTasks3 = new HashMap<>();
        JobConfig.TaskInfo taskInfo3 = new JobConfig.TaskInfo();
        taskInfo3.setType(TaskType.HIVE);
        loadTasks3.put("task3", taskInfo3);
        jobConfig.setLoadTasks(loadTasks3);
        IllegalArgumentException e4 =
                Assertions.assertThrows(IllegalArgumentException.class, jobConfig::checkTaskInfo);
        Assertions.assertEquals("hive database is empty", e4.getMessage());

        taskInfo3.setHiveDatabase("db");
        Assertions.assertThrows(IllegalArgumentException.class, jobConfig::checkTaskInfo, "hive table is empty");

        taskInfo3.setHiveTable("tbl");
        Assertions.assertDoesNotThrow(jobConfig::checkTaskInfo);

        new MockUp<DorisClient.FeClient>(DorisClient.FeClient.class) {
            @Mock
            public String getDDL(String db, String table) throws SparkLoadException {
                return "create table tbl1 (col1 int, col2 int, col3 int, col4 int) unique key (col1) properties (" +
                        "\"enable_unique_key_merge_on_write\" = \"true\")";
            }
        };
        IllegalArgumentException e5 =
                Assertions.assertThrows(IllegalArgumentException.class, jobConfig::checkTaskInfo);

    }

    @Test
    public void checkSparkInfo() throws IOException {

        JobConfig jobConfig = new JobConfig();
        JobConfig.SparkInfo sparkInfo = new JobConfig.SparkInfo();
        jobConfig.setSpark(sparkInfo);
        IllegalArgumentException e1 =
                Assertions.assertThrows(IllegalArgumentException.class, jobConfig::checkSparkInfo);
        Assertions.assertEquals("spark config item sparkHome is empty", e1.getMessage());

        sparkInfo.setSparkHome("test");
        IllegalArgumentException e2 =
                Assertions.assertThrows(IllegalArgumentException.class, jobConfig::checkSparkInfo);
        Assertions.assertEquals("spark master only supports yarn or standalone or local", e2.getMessage());

        sparkInfo.setMaster("local");
        sparkInfo.setDeployMode("abc");
        IllegalArgumentException e3 =
                Assertions.assertThrows(IllegalArgumentException.class, jobConfig::checkSparkInfo);
        Assertions.assertEquals("spark deployMode only supports cluster or client", e3.getMessage());

        sparkInfo.setMaster("spark://127.0.0.1:7077");
        sparkInfo.setDeployMode("cluster");
        IllegalArgumentException e4 =
                Assertions.assertThrows(IllegalArgumentException.class, jobConfig::checkSparkInfo);
        Assertions.assertEquals("standalone and local master only supports client mode", e4.getMessage());

        sparkInfo.setMaster("yarn");
        sparkInfo.setDeployMode("cluster");
        IllegalArgumentException e5 =
                Assertions.assertThrows(IllegalArgumentException.class, jobConfig::checkSparkInfo);
        Assertions.assertEquals("dpp jar file is not exists, path: null/app/spark-load-dpp-1.0-SNAPSHOT.jar", e5.getMessage());

        sparkInfo.setDppJarPath("");
        IllegalArgumentException e6 =
                Assertions.assertThrows(IllegalArgumentException.class, jobConfig::checkSparkInfo);
        Assertions.assertEquals("dpp jar file path is empty", e6.getMessage());

        Path path = Files.createTempFile(null, null);
        sparkInfo.setDppJarPath(path.toAbsolutePath().toString());
        Assertions.assertDoesNotThrow(jobConfig::checkSparkInfo);

    }

    @Test
    public void checkHadoopProperties() throws IOException {

        JobConfig jobConfig = new JobConfig();
        Map<String, String> hadoopProperties = new HashMap<>();
        jobConfig.setHadoopProperties(hadoopProperties);

        hadoopProperties.put("abc", "123");
        IllegalArgumentException e1 =
                Assertions.assertThrows(IllegalArgumentException.class, jobConfig::checkHadoopProperties);
        Assertions.assertEquals("fs.defaultFS is empty", e1.getMessage());

        hadoopProperties.put("fs.defaultFS", "test");
        IllegalArgumentException e2 =
                Assertions.assertThrows(IllegalArgumentException.class, jobConfig::checkHadoopProperties);
        Assertions.assertEquals("hadoop username is empty", e2.getMessage());

        hadoopProperties.put("hadoop.username", "hadoop");
        Assertions.assertDoesNotThrow(jobConfig::checkHadoopProperties);

        hadoopProperties.put("hadoop.security.authentication", "kerberos");
        IllegalArgumentException e3 =
                Assertions.assertThrows(IllegalArgumentException.class, jobConfig::checkHadoopProperties);
        Assertions.assertEquals("hadoop.kerberos.principal is not set", e3.getMessage());

        hadoopProperties.put("hadoop.kerberos.principal", "");
        IllegalArgumentException e4 =
                Assertions.assertThrows(IllegalArgumentException.class, jobConfig::checkHadoopProperties);
        Assertions.assertEquals("hadoop kerberos principal is empty", e4.getMessage());

        hadoopProperties.put("hadoop.kerberos.principal", "spark@DORIS.ORG");
        IllegalArgumentException e5 =
                Assertions.assertThrows(IllegalArgumentException.class, jobConfig::checkHadoopProperties);
        Assertions.assertEquals("hadoop.kerberos.keytab is not set", e5.getMessage());

        hadoopProperties.put("hadoop.kerberos.keytab", "test");
        IllegalArgumentException e6 =
                Assertions.assertThrows(IllegalArgumentException.class, jobConfig::checkHadoopProperties);
        Assertions.assertEquals("hadoop kerberos keytab file is not exists, path: test", e6.getMessage());

        Path path = Files.createTempFile("spark", ".keytab");
        hadoopProperties.put("hadoop.kerberos.keytab", path.toAbsolutePath().toString());
        Assertions.assertDoesNotThrow(jobConfig::checkHadoopProperties);

    }
}