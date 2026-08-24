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

package org.apache.doris.spark.config;

import org.apache.doris.spark.exception.OptionRequiredException;

import java.io.Serializable;
import java.util.Map;

/** Immutable configuration for the S3 TVF sink. */
public final class S3TvfOptions implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final String FORMAT = "format";
    private static final String READ_JSON_BY_LINE = "read_json_by_line";

    private final String endpoint;
    private final String region;
    private final String bucket;
    private final String prefix;
    private final String accessKey;
    private final String secretKey;
    private final boolean pathStyleAccess;

    private S3TvfOptions(
            String endpoint,
            String region,
            String bucket,
            String prefix,
            String accessKey,
            String secretKey,
            boolean pathStyleAccess) {
        this.endpoint = endpoint;
        this.region = region;
        this.bucket = bucket;
        this.prefix = prefix;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.pathStyleAccess = pathStyleAccess;
    }

    public static S3TvfOptions fromConfig(DorisConfig config) throws OptionRequiredException {
        validateLoadProperties(config.getSinkProperties());
        String endpoint = required(config, DorisOptions.DORIS_SINK_S3_ENDPOINT);
        String region = required(config, DorisOptions.DORIS_SINK_S3_REGION);
        String bucket = required(config, DorisOptions.DORIS_SINK_S3_BUCKET);
        String prefix = required(config, DorisOptions.DORIS_SINK_S3_PREFIX);
        String accessKey = required(config, DorisOptions.DORIS_SINK_S3_ACCESS_KEY);
        String secretKey = required(config, DorisOptions.DORIS_SINK_S3_SECRET_KEY);
        validatePrefix(prefix);

        return new S3TvfOptions(
                endpoint,
                region,
                bucket,
                prefix,
                accessKey,
                secretKey,
                config.getValue(DorisOptions.DORIS_SINK_S3_PATH_STYLE_ACCESS));
    }

    private static String required(DorisConfig config, ConfigOption<String> option)
            throws OptionRequiredException {
        String value = config.getValue(option).trim();
        if (value.isEmpty()) {
            throw new IllegalArgumentException(option.getName() + " must not be empty");
        }
        return value;
    }

    private static void validateLoadProperties(Map<String, String> loadProperties) {
        String format = loadProperties.get(FORMAT);
        if (format != null && !"json".equalsIgnoreCase(format.trim())) {
            throw new IllegalArgumentException("TVF write mode only supports JSON format");
        }
        String readJsonByLine = loadProperties.get(READ_JSON_BY_LINE);
        if (readJsonByLine != null && !Boolean.parseBoolean(readJsonByLine.trim())) {
            throw new IllegalArgumentException(
                    "TVF write mode requires 'doris.sink.properties.read_json_by_line' to be true");
        }
    }

    private static void validatePrefix(String prefix) {
        for (char character : "*?[]{},\\".toCharArray()) {
            if (prefix.indexOf(character) >= 0) {
                throw new IllegalArgumentException(
                        DorisOptions.DORIS_SINK_S3_PREFIX.getName()
                                + " must not contain glob characters");
            }
        }
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getRegion() {
        return region;
    }

    public String getBucket() {
        return bucket;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public boolean isPathStyleAccess() {
        return pathStyleAccess;
    }
}
