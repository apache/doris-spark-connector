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

import org.apache.doris.common.enums.JobStatus;
import org.apache.doris.config.JobConfig;
import org.apache.doris.exception.SparkLoadException;

import lombok.Getter;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

public abstract class Loader {

    private static final String SPARK_HADOOP_PREFIX = "spark.hadoop.";

    protected JobConfig jobConfig;

    protected boolean isRecoveryMode = false;

    @Getter
    protected SparkAppHandle appHandle;

    @Getter
    protected JobStatus jobStatus = JobStatus.RUNNING;

    protected final Map<String, String> statusInfo = new HashMap<>();

    public abstract void prepare() throws SparkLoadException;

    public void execute() throws SparkLoadException {
        try {
            appHandle = submitSparkJob(getMainClass(), getAppArgs(), getLogPath());
        } catch (IOException e) {
            throw new SparkLoadException("submit spark job failed", e);
        }
        do {
            if (appHandle.getState().isFinal()) {
                if (SparkAppHandle.State.FAILED == appHandle.getState()
                        || SparkAppHandle.State.KILLED == appHandle.getState()) {
                    statusInfo.put("msg",
                            String.format("spark job run failed, appId: %s, state: %s", appHandle.getAppId(),
                                    appHandle.getState()));
                    jobStatus = JobStatus.FAILED;
                } else {
                    jobStatus = JobStatus.SUCCESS;
                }
                break;
            }
            statusInfo.put("appId", appHandle.getAppId());
            LockSupport.parkNanos(Duration.ofSeconds(5).toNanos());
        } while (true);
    }

    private SparkAppHandle submitSparkJob(String mainClass, String[] appArgs, String logPath) throws IOException {
        File logFile = new File(logPath);
        if (!logFile.getParentFile().exists()) {
            logFile.getParentFile().mkdir();
        }
        JobConfig.SparkInfo sparkInfo = jobConfig.getSpark();
        SparkLauncher launcher = new SparkLauncher(jobConfig.getEnv())
                .setMaster(sparkInfo.getMaster())
                .setDeployMode(sparkInfo.getDeployMode())
                .setAppName("spark-load-" + jobConfig.getLabel())
                .setAppResource(sparkInfo.getDppJarPath())
                .setSparkHome(sparkInfo.getSparkHome())
                .setMainClass(mainClass)
                .addAppArgs(appArgs)
                .redirectError(logFile);
        sparkInfo.getProperties().forEach(launcher::setConf);
        jobConfig.getHadoopProperties().forEach((k, v) -> launcher.setConf(SPARK_HADOOP_PREFIX + k, v));
        return launcher.startApplication();
    }

    public void cancel() {
        if (jobStatus == JobStatus.RUNNING) {
            if (appHandle != null) {
                try {
                    appHandle.stop();
                } catch (Exception e) {
                    appHandle.kill();
                }
            }
        }
        jobStatus = JobStatus.FAILED;
        afterFailed(new SparkLoadException("load client cancelled."));
    }

    protected abstract String getMainClass();

    protected abstract String[] getAppArgs();

    protected abstract String getLogPath();

    public abstract void afterFinished() throws SparkLoadException;

    public abstract void afterFailed(Exception e);

}
