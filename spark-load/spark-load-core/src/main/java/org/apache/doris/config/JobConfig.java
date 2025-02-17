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

import org.apache.doris.SparkLoadRunner;
import org.apache.doris.client.DorisClient;
import org.apache.doris.common.Constants;
import org.apache.doris.common.enums.LoadMode;
import org.apache.doris.common.enums.StorageType;
import org.apache.doris.common.enums.TaskType;
import org.apache.doris.exception.SparkLoadException;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.Data;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.net.URI;
import java.sql.DriverManager;
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
    private String workingDir;

    @JsonProperty(required = true)
    private Map<String, TaskInfo> loadTasks;

    @JsonProperty(required = true)
    private SparkInfo spark;

    private LoadMode loadMode = LoadMode.PULL;

    private Map<String, String> hadoopProperties = Collections.emptyMap();

    private Map<String, String> jobProperties = Collections.emptyMap();

    private Map<String, String> env = Collections.emptyMap();

    private StorageType storageType = StorageType.HDFS;

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

        private String fieldSep = "\t";

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
            String table = entry.getKey();
            try {
                DorisClient.FeClient feClient = DorisClient.getFeClient(feAddresses, user, password);
                String ddl = feClient.getDDL(database, table);
                if (StringUtils.isNoneBlank(ddl) && ddl.contains("\"enable_unique_key_merge_on_write\" = \"true\"")) {
                    throw new IllegalArgumentException("Merge On Write is not supported");
                }
            } catch (SparkLoadException e) {
                throw new IllegalArgumentException("check table failed", e);
            }
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
                        Preconditions.checkArgument(StringUtils.isNoneEmpty(taskInfo.getFieldSep()),
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
        Preconditions.checkArgument(checkSparkMaster(sparkInfo.getMaster()),
                "spark master only supports yarn or standalone or local");
        Preconditions.checkArgument(
                StringUtils.equalsAnyIgnoreCase(sparkInfo.getDeployMode(), "cluster", "client"),
                "spark deployMode only supports cluster or client");
        if (!"yarn".equalsIgnoreCase(sparkInfo.getMaster())) {
            Preconditions.checkArgument("client".equalsIgnoreCase(sparkInfo.getDeployMode()),
                    "standalone and local master only supports client mode");
        }
        if (LoadMode.PULL == getLoadMode()) {
            if (StringUtils.isBlank(sparkInfo.getDppJarPath())) {
                throw new IllegalArgumentException("dpp jar file path is empty");
            }
            if (!new File(sparkInfo.getDppJarPath()).exists()) {
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

    public void checkHadoopProperties() {
        if (hadoopProperties == null || hadoopProperties.isEmpty()) {
            return;
        }
        if (!workingDir.startsWith("s3")) {
            if (!hadoopProperties.containsKey("fs.defaultFS")) {
                throw new IllegalArgumentException("fs.defaultFS is empty");
            }
            // check auth
            if (hadoopProperties.containsKey("hadoop.security.authentication")
                    && StringUtils.equalsIgnoreCase(hadoopProperties.get("hadoop.security.authentication"), "kerberos")) {
                if (hadoopProperties.containsKey("hadoop.kerberos.principal")) {
                    if (StringUtils.isBlank(hadoopProperties.get("hadoop.kerberos.principal"))) {
                        throw new IllegalArgumentException("hadoop kerberos principal is empty");
                    }
                    if (hadoopProperties.containsKey("hadoop.kerberos.keytab")) {
                        if (!FileUtils.getFile(hadoopProperties.get("hadoop.kerberos.keytab")).exists()) {
                            throw new IllegalArgumentException("hadoop kerberos keytab file is not exists, path: "
                                    + hadoopProperties.get("hadoop.kerberos.keytab"));
                        }
                        return;
                    }
                    throw new IllegalArgumentException("hadoop.kerberos.keytab is not set");
                }
                throw new IllegalArgumentException("hadoop.kerberos.principal is not set");
            } else {
                if (!hadoopProperties.containsKey("hadoop.username")) {
                    throw new IllegalArgumentException("hadoop username is empty");
                }
            }
        }
    }

}
