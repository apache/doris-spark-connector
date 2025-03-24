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

package org.apache.doris;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.doris.common.CommandLineOptions;
import org.apache.doris.common.Constants;
import org.apache.doris.common.enums.StorageType;
import org.apache.doris.config.JobConfig;
import org.apache.doris.load.LoaderFactory;
import org.apache.doris.load.job.Loader;
import org.apache.doris.load.job.Recoverable;
import org.apache.doris.util.JsonUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4JLoggerFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class SparkLoadRunner {

    private static final Logger LOG = LogManager.getLogger(SparkLoadRunner.class);

    public static final String SPARK_LOAD_HOME = System.getenv("SPARK_LOAD_HOME");

    static {
        InternalLoggerFactory.setDefaultFactory(Log4JLoggerFactory.INSTANCE);
    }

    public static void main(String[] args) {

        if (StringUtils.isBlank(SPARK_LOAD_HOME)) {
            System.err.println("env SPARK_LOAD_HOME is not set.");
            System.exit(-1);
        }

        CommandLineOptions cmdOptions = parseArgs(args);
        if (Strings.isNullOrEmpty(cmdOptions.getConfigPath())) {
            System.err.println("config path is empty");
            System.exit(-1);
        }

        JobConfig jobConfig = readConfig(cmdOptions.getConfigPath());
        try {
            preprocessConfig(jobConfig);
            checkConfig(jobConfig);
        } catch (IllegalArgumentException e) {
            System.err.println("check config failed, msg: " + ExceptionUtils.getStackTrace(e));
            System.exit(-1);
        }

        Loader loader = LoaderFactory.createLoader(jobConfig, cmdOptions.getRecovery());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down...");
            loader.cancel();
        }));
        try {

            loader.prepare();
            do {
                if (loader instanceof Recoverable) {
                    if (((Recoverable) loader).canBeRecovered()) {
                        LOG.info("recovery check passed, start prepare recovery.");
                        ((Recoverable) loader).prepareRecover();
                        break;
                    }
                }
                loader.execute();
            } while (false);

            loader.afterFinished();
            // loader.cancel();

        } catch (Exception e) {
            loader.afterFailed(e);
            LOG.error("start load failed", e);
            System.err.println("start load failed, exit.");
            System.exit(-1);
        }

    }

    private static CommandLineOptions parseArgs(String[] args) {
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption("c", "config", true, "Spark load config file");
        options.addOption("r", "recovery", false, "Recovery mode");
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println("failed to parse argument, exit.");
            System.exit(-1);
        }

        String configPath = cmd.getOptionValue("config");
        boolean recovery = cmd.hasOption('r') || cmd.hasOption("recovery");
        return new CommandLineOptions(configPath, recovery);

    }

    private static JobConfig readConfig(String path) {
        JobConfig jobConfig = null;
        try {
            jobConfig = JsonUtils.readValue(new File(path), JobConfig.class);
        } catch (IOException e) {
            LOG.error("failed to read config file", e);
            System.err.println("failed to read config file, exit.");
            System.exit(-1);
        }
        return jobConfig;
    }

    private static void checkConfig(JobConfig jobConfig) {
        jobConfig.checkFeAddress();
        Preconditions.checkArgument(StringUtils.isNoneBlank(jobConfig.getLabel()), "label is empty");
        Preconditions.checkArgument(StringUtils.isNoneBlank(jobConfig.getUser()), "user is empty");
        Preconditions.checkArgument(jobConfig.getPassword() != null, "password cannot be null");
        Preconditions.checkArgument(StringUtils.isNoneBlank(jobConfig.getDatabase()), "database is empty");
        Preconditions.checkArgument(StringUtils.isNoneBlank(jobConfig.getWorkingDir()),
                "spark config item workingDir is empty");
        jobConfig.checkTaskInfo();
        jobConfig.checkSparkInfo();
        jobConfig.checkHadoopProperties();
    }

    private static void preprocessConfig(JobConfig jobConfig) {
        loadHadoopConfig(jobConfig);
        handleS3Config(jobConfig);
    }

    protected static void loadHadoopConfig(JobConfig jobConfig) {
        if (jobConfig.getEnv().containsKey("HADOOP_CONF_DIR")) {
            Configuration conf = new Configuration();
            String hadoopConfDir = jobConfig.getEnv().get("HADOOP_CONF_DIR");
            if (new File(hadoopConfDir + "/core-site.xml").exists()) {
                System.out.println("core-site.xml was found at " + hadoopConfDir + "/core-site.xml");
                conf.addResource(new Path(hadoopConfDir, "core-site.xml"));
            }
            if (new File(hadoopConfDir + "/hdfs-site.xml").exists()) {
                System.out.println("hdfs-site.xml was found at " + hadoopConfDir + "/hdfs-site.xml");
                conf.addResource(new Path(hadoopConfDir, "hdfs-site.xml"));
            }
            if (new File(hadoopConfDir + "/yarn-site.xml").exists()) {
                System.out.println("yarn-site.xml was found at " + hadoopConfDir + "/yarn-site.xml");
                conf.addResource(new Path(hadoopConfDir, "yarn-site.xml"));
            }
            Map<String, String> newHadoopProps = new HashMap<>();
            for (Map.Entry<String, String> confEntry : conf) {
                newHadoopProps.put(confEntry.getKey(), confEntry.getValue());
            }
            newHadoopProps.putAll(jobConfig.getHadoopProperties());
            jobConfig.setHadoopProperties(newHadoopProps);
        }
    }

    private static void handleS3Config(JobConfig jobConfig) {
        URI uri = URI.create(jobConfig.getWorkingDir());
        if (uri.getScheme().equalsIgnoreCase("s3")) {

            Map<String, String> hadoopProperties = new HashMap<>(jobConfig.getHadoopProperties());
            Preconditions.checkArgument(hadoopProperties.containsKey(Constants.S3_ENDPOINT), "s3.endpoint is empty");
            Preconditions.checkArgument(hadoopProperties.containsKey(Constants.S3_REGION), "s3.region is empty");
            Preconditions.checkArgument(hadoopProperties.containsKey(Constants.S3_ACCESS_KEY), "s3.access_key is empty");
            Preconditions.checkArgument(hadoopProperties.containsKey(Constants.S3_SECRET_KEY), "s3.secret_key is empty");

            hadoopProperties.put("fs.s3a.endpoint", hadoopProperties.get(Constants.S3_ENDPOINT));
            hadoopProperties.remove(Constants.S3_ENDPOINT);
            hadoopProperties.put("fs.s3a.endpoint.region", hadoopProperties.get(Constants.S3_REGION));
            hadoopProperties.remove(Constants.S3_REGION);
            hadoopProperties.put("fs.s3a.access.key", hadoopProperties.get(Constants.S3_ACCESS_KEY));
            hadoopProperties.remove(Constants.S3_ACCESS_KEY);
            hadoopProperties.put("fs.s3a.secret.key", hadoopProperties.get(Constants.S3_SECRET_KEY));
            hadoopProperties.remove(Constants.S3_SECRET_KEY);
            if (hadoopProperties.containsKey(Constants.S3_TOKEN)) {
                hadoopProperties.put("fs.s3a.session.token", hadoopProperties.get(Constants.S3_TOKEN));
                hadoopProperties.remove(Constants.S3_TOKEN);
                hadoopProperties.put("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
            } else {
                hadoopProperties.put("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
            }
            hadoopProperties.put("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            jobConfig.setHadoopProperties(hadoopProperties);

            //  working dir path replace s3:// with s3a://
            String resolvedWorkingDir = "s3a:" + uri.getSchemeSpecificPart();
            jobConfig.setWorkingDir(resolvedWorkingDir);

            // load task path replace s3:// with s3a://
            for (String s : jobConfig.getLoadTasks().keySet()) {
                JobConfig.TaskInfo taskInfo = jobConfig.getLoadTasks().get(s);
                List<String> resolvedPaths = taskInfo.getPaths().stream()
                        .map(path -> "s3a:" + URI.create(path).getSchemeSpecificPart())
                        .collect(Collectors.toList());
                taskInfo.setPaths(resolvedPaths);
            }
            jobConfig.setStorageType(StorageType.S3);

        }
    }

}
