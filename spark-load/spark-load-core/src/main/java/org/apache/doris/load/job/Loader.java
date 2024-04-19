package org.apache.doris.load.job;

import org.apache.doris.common.JobStatus;
import org.apache.doris.common.SparkLoadException;
import org.apache.doris.config.JobConfig;

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
            if (SparkAppHandle.State.FAILED == appHandle.getState()
                    || SparkAppHandle.State.KILLED == appHandle.getState()
                    || SparkAppHandle.State.FINISHED == appHandle.getState()) {
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

    protected abstract String getMainClass();

    protected abstract String[] getAppArgs();

    protected abstract String getLogPath();

    public abstract void afterFinished() throws SparkLoadException;

    public abstract void afterFailed(Exception e);

}
