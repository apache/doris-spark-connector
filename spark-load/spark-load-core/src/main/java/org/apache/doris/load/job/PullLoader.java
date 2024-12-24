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

package org.apache.doris.load.job;

import org.apache.doris.SparkLoadRunner;
import org.apache.doris.client.DorisClient;
import org.apache.doris.common.Constants;
import org.apache.doris.common.DppResult;
import org.apache.doris.common.LoadInfo;
import org.apache.doris.common.enums.JobStatus;
import org.apache.doris.common.meta.LoadMeta;
import org.apache.doris.common.meta.TableMeta;
import org.apache.doris.config.EtlJobConfig;
import org.apache.doris.config.JobConfig;
import org.apache.doris.exception.SparkLoadException;
import org.apache.doris.util.DateUtils;
import org.apache.doris.util.FileSystemUtils;
import org.apache.doris.util.JsonUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

public class PullLoader extends Loader implements Recoverable {

    private static final Logger LOG = LogManager.getLogger(PullLoader.class);

    private static final String LOAD_META_JSON = "load_meta.json";

    private static final String DPP_RESULT_JSON = "dpp_result.json";

    private static final String SPARK_ETL_JOB_CLASS = "org.apache.doris.load.loadv2.etl.SparkEtlJob";

    private LoadMeta loadMeta;

    private EtlJobConfig etlJobConfig;

    public PullLoader(JobConfig jobConfig, Boolean isRecoveryMode) {
        this.jobConfig = jobConfig;
        this.isRecoveryMode = isRecoveryMode;
    }

    @Override
    public void prepare() throws SparkLoadException {
        DorisClient.FeClient feClient = DorisClient.getFeClient(jobConfig.getFeAddresses(), jobConfig.getUser(),
                jobConfig.getPassword());
        Map<String, List<String>> tableToPartition = jobConfig.getLoadTasks().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getTargetPartitions()));
        loadMeta = feClient.createIngestionLoad(jobConfig.getDatabase(), tableToPartition, jobConfig.getLabel(),
                jobConfig.getJobProperties());
        etlJobConfig = loadMeta.getEtlJobConfig(jobConfig);
        if (Constants.HADOOP_AUTH_KERBEROS.equalsIgnoreCase(
                jobConfig.getHadoopProperties().get(Constants.HADOOP_SECURITY_AUTHENTICATION))) {
            try {
                FileSystemUtils.kerberosLogin(jobConfig);
            } catch (IOException e) {
                throw new SparkLoadException("login with kerberos auth failed", e);
            }
        }
    }

    @Override
    public void execute() throws SparkLoadException {

        try {
            cleanOutputPath();
        } catch (IOException e) {
            throw new SparkLoadException("clean output path failed", e);
        }
        uploadMetaInfo(loadMeta, etlJobConfig.getOutputPath());

        String etlJobConfPath = etlJobConfig.outputPath + "/configs/jobconfig.json";
        try {
            FileSystemUtils.createFile(jobConfig, etlJobConfig.configToJson(), etlJobConfPath, true);
        } catch (IOException e) {
            throw new SparkLoadException("create job config file failed", e);
        }

        JobConfig.SparkInfo spark = jobConfig.getSpark();

        LOG.info("submit spark job on master: " + spark.getMaster() + ", deployMode: " + spark.getDeployMode());

        super.execute();

        if (jobStatus == JobStatus.FAILED) {
            throw new SparkLoadException("spark job run failed, msg: " + statusInfo.get("msg"));
        }
        LOG.info("spark job run finished.");

    }

    @Override
    public void afterFinished() throws SparkLoadException {
        DorisClient.FeClient feClient = DorisClient.getFeClient(jobConfig.getFeAddresses(), jobConfig.getUser(),
                jobConfig.getPassword());
        statusInfo.put("status", jobStatus.name());
        statusInfo.put("msg", "");
        statusInfo.put("appId", appHandle == null ? null : appHandle.getAppId());
        try {
            String dppResultStr = null;
            int checkCnt = 0;
            while (checkCnt < 3) {
                try {
                    dppResultStr = getDppResultString();
                } catch (UnsupportedOperationException e) {
                    LOG.warn("retry get dpp result", e);
                    checkCnt++;
                    LockSupport.parkNanos(Duration.ofMillis(500).toNanos());
                }
                if (dppResultStr != null) {
                    break;
                }
            }
            if (dppResultStr == null) {
                throw new SparkLoadException("get dpp result str failed");
            }
            statusInfo.put("dppResult", dppResultStr);
            statusInfo.put("filePathToSize", JsonUtils.writeValueAsString(getFilePathToSize()));
            statusInfo.put("hadoopProperties", JsonUtils.writeValueAsString(jobConfig.getHadoopProperties()));
        } catch (IOException e) {
            throw new SparkLoadException("update job status failed", e);
        }
        feClient.updateIngestionLoad(jobConfig.getDatabase(), loadMeta.getLoadId(), statusInfo);
        do {
            LoadInfo loadInfo = feClient.getLoadInfo(jobConfig.getDatabase(), jobConfig.getLabel());
            switch (loadInfo.getState().toUpperCase(Locale.ROOT)) {
                case "FINISHED":
                    LOG.info("load job finished.");
                    try {
                        cleanOutputPath();
                    } catch (IOException e) {
                        LOG.warn("clean output path failed", e);
                    }
                    return;
                case "CANCELLED":
                    throw new SparkLoadException("load job failed, " + loadInfo.getFailMsg());
                default:
                    LOG.info("load job unfinished, state: " + loadInfo.getState());
                    break;
            }
            LockSupport.parkNanos(Duration.ofSeconds(15).toNanos());
        } while (true);
    }

    @Override
    public void afterFailed(Exception e) {
        if (loadMeta == null) {
            LOG.info("load job not start, skip update.");
            return;
        }
        DorisClient.FeClient feClient = DorisClient.getFeClient(jobConfig.getFeAddresses(), jobConfig.getUser(),
                jobConfig.getPassword());
        statusInfo.put("status", jobStatus.name());
        statusInfo.put("msg", e.getMessage());
        statusInfo.put("appId", appHandle == null ? null : appHandle.getAppId());
        try {
            feClient.updateIngestionLoad(jobConfig.getDatabase(), loadMeta.getLoadId(), statusInfo);
        } catch (SparkLoadException ex) {
            LOG.warn("update load failed status failed", ex);
        }
    }

    @Override
    public boolean canBeRecovered() throws SparkLoadException {
        if (isRecoveryMode) {
            String outputPath = etlJobConfig.getOutputPath();
            String parentOutputPath = outputPath.substring(0, StringUtils.lastIndexOf(outputPath, "/"));
            try {
                if (FileSystemUtils.exists(jobConfig, parentOutputPath)) {
                    FileStatus[] fileStatuses = FileSystemUtils.list(jobConfig, parentOutputPath);
                    if (fileStatuses.length != 1) {
                        return false;
                    }
                    fileStatuses = FileSystemUtils.list(jobConfig, fileStatuses[0].getPath().toString());
                    boolean hasDppResult = false;
                    for (FileStatus fileStatus : fileStatuses) {
                        String fileName = fileStatus.getPath().getName();
                        if (DPP_RESULT_JSON.equalsIgnoreCase(fileName)) {
                            hasDppResult = true;
                            String content = FileSystemUtils.readFile(jobConfig, fileStatus.getPath().toString());
                            if (StringUtils.isBlank(content)) {
                                return false;
                            }
                            DppResult dppResult = JsonUtils.readValue(content, DppResult.class);
                            if (!checkDppResult(dppResult)) {
                                LOG.info("previous etl job is failed, cannot be recovered");
                                return false;
                            }
                        }
                        // check meta consist
                        if (LOAD_META_JSON.equalsIgnoreCase(fileName)) {
                            String content = FileSystemUtils.readFile(jobConfig, fileStatus.getPath().toString());
                            if (StringUtils.isBlank(content)) {
                                return false;
                            }
                            LoadMeta oldLoadMeta = JsonUtils.readValue(content, LoadMeta.class);
                            for (Map.Entry<String, TableMeta> entry : loadMeta.getTableMeta().entrySet()) {
                                TableMeta tableMeta = entry.getValue();
                                TableMeta oldTableMeta = oldLoadMeta.getTableMeta().get(entry.getKey());
                                // index count is not consistent
                                if (oldTableMeta == null
                                        || oldTableMeta.getIndexes().size() != tableMeta.getIndexes().size()) {
                                    LOG.info("index size mismatch, cannot be recovered");
                                    return false;
                                }
                                Map<Long, EtlJobConfig.EtlIndex> indexMap = tableMeta.getIndexes().stream()
                                        .collect(Collectors.toMap(etlIndex -> etlIndex.indexId,
                                                TableMeta.EtlIndex::toEtlIndex));
                                Map<Long, EtlJobConfig.EtlIndex> oldIndexMap = oldTableMeta.getIndexes().stream()
                                        .collect(Collectors.toMap(etlIndex -> etlIndex.indexId,
                                                TableMeta.EtlIndex::toEtlIndex));
                                for (Map.Entry<Long, EtlJobConfig.EtlIndex> indexEntry : indexMap.entrySet()) {
                                    EtlJobConfig.EtlIndex index = indexEntry.getValue();
                                    EtlJobConfig.EtlIndex oldIndex = oldIndexMap.get(indexEntry.getKey());
                                    // index not exists
                                    if (oldIndex == null) {
                                        LOG.info("index " + index.indexId + " is not exists in previous meta");
                                        return false;
                                    }
                                    // index mismatch
                                    if (oldIndex.schemaHash != index.schemaHash
                                            || oldIndex.schemaVersion != index.schemaVersion) {
                                        LOG.info("index " + index.indexId + " has changed, "
                                                + "old schemaHash: " + oldIndex.schemaHash + " and schemaVersion: "
                                                + oldIndex.schemaVersion + " current schemaHash: "
                                                + index.schemaHash + " and schemaVersion: "
                                                + index.schemaVersion + ", cannot be recovered");
                                        return false;
                                    }
                                }
                                // check partition consistent
                                Set<Long> partitionSet = tableMeta.getPartitionInfo().partitions.stream().map(
                                        p -> p.partitionId).collect(Collectors.toSet());
                                Set<Long> oldPartitionSet = oldTableMeta.getPartitionInfo().partitions.stream().map(
                                        p -> p.partitionId).collect(Collectors.toSet());
                                if (oldPartitionSet.size() != partitionSet.size()) {
                                    LOG.info("partition size mismatch, old partition size: " + oldPartitionSet.size()
                                            + ", now partition size: " + partitionSet.size()
                                            + ", cannot be recovered");
                                    return false;
                                }
                                for (Long partitionId : partitionSet) {
                                    if (!oldPartitionSet.contains(partitionId)) {
                                        LOG.info("partition id mismatch, partition id: " + partitionId
                                                + ", cannot be recovered");
                                        return false;
                                    }
                                }
                            }
                        }
                    }
                    return hasDppResult;
                }
            } catch (IOException e) {
                throw new SparkLoadException("check recovery failed", e);
            }
        }
        return false;
    }

    @Override
    public void prepareRecover() throws SparkLoadException {
        String outputPath = etlJobConfig.getOutputPath();
        String parentOutputPath = outputPath.substring(0, StringUtils.lastIndexOf(outputPath, "/"));
        try {
            FileStatus[] fileStatuses = FileSystemUtils.list(jobConfig, parentOutputPath);
            FileSystemUtils.move(jobConfig, fileStatuses[0].getPath().toString(), outputPath);
            FileSystemUtils.delete(jobConfig, outputPath + "/load_meta.json");
            uploadMetaInfo(loadMeta, etlJobConfig.getOutputPath());
            jobStatus = JobStatus.SUCCESS;
        } catch (IOException e) {
            throw new SparkLoadException("prepare recovery failed", e);
        }
    }

    private boolean checkDppResult(DppResult dppResult) {
        if (!dppResult.isSuccess) {
            return false;
        }
        int maxFilterRatio = Integer.parseInt(jobConfig.getJobProperties().getOrDefault("max_filter_ratio", "0"));
        return dppResult.abnormalRows <= (dppResult.abnormalRows + dppResult.normalRows) * maxFilterRatio;
    }

    private void uploadMetaInfo(LoadMeta metaInfo, String outputPath) throws SparkLoadException {
        try {
            if (!FileSystemUtils.exists(jobConfig, outputPath)) {
                FileSystemUtils.mkdir(jobConfig, outputPath);
            }
            FileSystemUtils.createFile(jobConfig, JsonUtils.writeValueAsBytes(metaInfo),
                    outputPath + "/load_meta.json", true);
        } catch (IOException e) {
            throw new SparkLoadException("upload load meta failed", e);
        }
    }

    @Override
    protected String getMainClass() {
        return SPARK_ETL_JOB_CLASS;
    }

    @Override
    protected String[] getAppArgs() {
        return new String[] {etlJobConfig.outputPath + "/configs/jobconfig.json"};
    }

    @Override
    protected String getLogPath() {
        String formattedNow = DateUtils.getFormattedNow(DateUtils.NUMBER_FORMATER);
        return SparkLoadRunner.SPARK_LOAD_HOME + "/logs/" + jobConfig.getLabel() + "-" + formattedNow + ".log";
    }

    public void cleanOutputPath() throws IOException {
        if (FileSystemUtils.exists(jobConfig, etlJobConfig.outputPath)) {
            LOG.info("clean output: " + etlJobConfig.outputPath);
            FileSystemUtils.delete(jobConfig, etlJobConfig.outputPath);
        }
    }

    private String getDppResultString() throws SparkLoadException {
        try {
            return FileSystemUtils.readFile(jobConfig, etlJobConfig.outputPath + "/dpp_result.json");
        } catch (IOException e) {
            throw new SparkLoadException("get dpp result failed", e);
        }
    }

    private Map<String, Long> getFilePathToSize() throws SparkLoadException {
        Map<String, Long> filePathToSize = new HashMap<>();
        try {
            FileStatus[] fileStatuses = FileSystemUtils.list(jobConfig, etlJobConfig.outputPath);
            for (FileStatus fileStatus : fileStatuses) {
                if (fileStatus.isDirectory()) {
                    continue;
                }
                filePathToSize.put(fileStatus.getPath().toString(), fileStatus.getLen());
            }
        } catch (IOException e) {
            throw new SparkLoadException("get dpp result failed", e);
        }
        return filePathToSize;
    }

}
