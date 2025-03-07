package org.apache.doris;

import org.apache.doris.config.JobConfig;

import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class SparkLoadRunnerTest {

    @Test
    void loadHadoopConfig() {

        JobConfig jobConfig = new JobConfig();
        Map<String, String> envMap = new HashMap<>();
        envMap.put("HADOOP_CONF_DIR", this.getClass().getResource("/").getPath());
        jobConfig.setEnv(envMap);
        SparkLoadRunner.loadHadoopConfig(jobConfig);
        Assertions.assertEquals("hdfs://my-hadoop/", jobConfig.getHadoopProperties().get("fs.defaultFS"));
        Assertions.assertEquals("1", jobConfig.getHadoopProperties().get("dfs.replication"));
        Assertions.assertEquals("my.hadoop.com", jobConfig.getHadoopProperties().get("yarn.resourcemanager.address"));

    }
}