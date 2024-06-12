package org.apache.doris.config;

import org.apache.doris.SparkLoadRunner;
import org.apache.doris.common.Constants;
import org.apache.doris.common.enums.LoadMode;
import org.apache.doris.sparkdpp.EtlJobConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class JobConfig {

    @JsonProperty(required = true)
    private String feAddresses;

    @JsonProperty(required = true)
    private String label;

    @JsonProperty(required = true)
    private String user;

    @JsonProperty(required = true)
    private String password;

    @JsonProperty(required = true)
    private String database;

    @JsonProperty(required = true)
    private Map<String, TaskInfo> loadTasks;

    @JsonProperty(required = true)
    private SparkInfo spark;

    private LoadMode loadMode = LoadMode.PULL;

    private Map<String, String> hadoopProperties = Collections.emptyMap();

    private Map<String, String> jobProperties = Collections.emptyMap();

    private Map<String, String> env = Collections.emptyMap();

    @Data
    public static class TaskInfo {

        private TaskType type;

        private String hiveMetastoreUris;

        private String hiveDatabase;

        private String hiveTable;

        private List<String> paths;

        private String format;

        private String columns;

        private String columnFromPath;

        private String fieldSep;

        private String lineDelim = "\n";

        private List<String> columnMappings = Collections.emptyList();

        private String where;

        private List<String> targetPartitions = Collections.emptyList();

        public String getHiveFullTableName() {
            return hiveDatabase + "." + hiveTable;
        }

        public Map<String, EtlJobConfig.EtlColumnMapping> toEtlColumnMappingMap() {
            Map<String, EtlJobConfig.EtlColumnMapping> map = new HashMap<>();
            for (String columnMapping : columnMappings) {
                String[] arr = columnMapping.split("=");
                map.put(arr[0], new EtlJobConfig.EtlColumnMapping(arr[1]));
            }
            return map;
        }

    }

    @Data
    public static class SparkInfo {

        private static final String DEFAULT_DEPLOY_MODE = "client";

        private static final String DEFAULT_DPP_JAR_PATH =
                SparkLoadRunner.SPARK_LOAD_HOME + "/app/spark-load-dpp-1.0-SNAPSHOT.jar";

        private String sparkHome;

        private String workingDir;

        private String master;

        private String deployMode = DEFAULT_DEPLOY_MODE;

        private Integer numExecutors;

        private Integer executorCores;

        private String executorMemory;

        private String driverMemory;

        private String dppJarPath = DEFAULT_DPP_JAR_PATH;

        private Map<String, String> properties = Collections.emptyMap();

    }

    public void checkFeAddress() {
        Preconditions.checkArgument(StringUtils.isNoneBlank(getFeAddresses()), "feAddress is empty");
        String[] feAddressArr = getFeAddresses().split(",");
        if (feAddressArr.length == 0) {
            throw new IllegalArgumentException("feAddress format is incorrect");
        }
        for (String feAddress : feAddressArr) {
            String[] arr = feAddress.split(":");
            if (arr.length != 2) {
                throw new IllegalArgumentException("feAddress format is incorrect");
            }
        }
    }

    public void checkTaskInfo() {
        Map<String, TaskInfo> tasks = getLoadTasks();
        Preconditions.checkArgument(!tasks.isEmpty(), "loadTasks is empty");
        for (Map.Entry<String, TaskInfo> entry : tasks.entrySet()) {
            TaskInfo taskInfo = entry.getValue();
            switch (taskInfo.getType()) {
                case HIVE:
                    Preconditions.checkArgument(StringUtils.isNoneBlank(taskInfo.getHiveDatabase()),
                            "hive database is empty");
                    Preconditions.checkArgument(StringUtils.isNoneBlank(taskInfo.getHiveTable()),
                            "hive table is empty");
                    break;
                case FILE:
                    Preconditions.checkArgument(taskInfo.getPaths() != null && !taskInfo.getPaths().isEmpty(),
                            "file path is empty");
                    Preconditions.checkArgument(
                            StringUtils.equalsAnyIgnoreCase(taskInfo.getFormat(), "parquet", "orc", "csv"),
                            "format only support parquet or orc or csv");
                    if ("csv".equalsIgnoreCase(taskInfo.getFormat())) {
                        Preconditions.checkArgument(StringUtils.isNoneBlank(taskInfo.getFieldSep()),
                                "field separator is empty");
                    }
                    break;
                default:
                    throw new IllegalArgumentException("task type only supports hive or file");
            }
        }
    }

    public void checkSparkInfo() {
        SparkInfo sparkInfo = getSpark();
        Preconditions.checkArgument(StringUtils.isNoneBlank(sparkInfo.getSparkHome()),
                "spark config item sparkHome is empty");
        Preconditions.checkArgument(StringUtils.isNoneBlank(sparkInfo.getWorkingDir()),
                "spark config item workingDir is empty");
        Preconditions.checkArgument(checkSparkMaster(sparkInfo.getMaster()),
                "spark master only supports yarn or standalone or local ");
        Preconditions.checkArgument(
                StringUtils.equalsAnyIgnoreCase(sparkInfo.getDeployMode(), "cluster", "client"),
                "spark deployMode only supports cluster or client ");
        if (!"yarn".equalsIgnoreCase(sparkInfo.getMaster())) {
            Preconditions.checkArgument("client".equalsIgnoreCase(sparkInfo.getDeployMode()),
                    "standalone and local master only supports client mode");
        }
        if (LoadMode.PULL == getLoadMode()) {
            if (StringUtils.isBlank(getSpark().getDppJarPath())) {
                throw new IllegalArgumentException("dpp jar file path is empty ");
            }
            if (!new File(getSpark().getDppJarPath()).exists()) {
                throw new IllegalArgumentException("dpp jar file is not exists, path: " + getSpark().getDppJarPath());
            }
        }
    }

    private boolean checkSparkMaster(String master) {
        if (StringUtils.isBlank(master)) {
            return false;
        }
        if ("yarn".equalsIgnoreCase(master) || master.startsWith("local")) {
            return true;
        }
        URI uri = URI.create(master);
        return Constants.SPARK_STANDALONE_SCHEME.equalsIgnoreCase(uri.getScheme())
                && StringUtils.isNoneBlank(uri.getHost()) && uri.getPort() != -1;
    }

}
