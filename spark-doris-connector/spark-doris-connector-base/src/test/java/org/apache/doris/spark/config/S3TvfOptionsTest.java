// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.spark.config;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class S3TvfOptionsTest {

    @Test
    public void supportsIamRoleAndDefaultGzip() throws Exception {
        S3TvfOptions options = S3TvfOptions.fromConfig(configWithRole());

        Assert.assertEquals("arn:aws:iam::123456789012:role/doris", options.getRoleArn());
        Assert.assertEquals("external-id", options.getExternalId());
        Assert.assertTrue(options.isGzipCompressionEnabled());
        Assert.assertFalse(options.hasStaticCredentials());
    }

    @Test
    public void allowsStaticSourceCredentialsAndDisablingGzip() throws Exception {
        Map<String, String> values = baseOptions();
        values.put("doris.sink.s3.access-key", "access-key");
        values.put("doris.sink.s3.secret-key", "secret-key");
        values.put("doris.sink.s3.role-arn", "arn:aws:iam::123456789012:role/doris");
        values.put("doris.sink.properties.compress_type", "");

        S3TvfOptions options = S3TvfOptions.fromConfig(DorisConfig.fromMap(values, false));

        Assert.assertTrue(options.hasStaticCredentials());
        Assert.assertFalse(options.isGzipCompressionEnabled());
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsMissingCredentialsAndRole() throws Exception {
        S3TvfOptions.fromConfig(DorisConfig.fromMap(baseOptions(), false));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsUnsupportedCompression() throws Exception {
        Map<String, String> values = baseOptions();
        values.put("doris.sink.s3.role-arn", "arn:aws:iam::123456789012:role/doris");
        values.put("doris.sink.properties.compress_type", "zstd");
        S3TvfOptions.fromConfig(DorisConfig.fromMap(values, false));
    }

    public static DorisConfig configWithRole() throws Exception {
        Map<String, String> values = baseOptions();
        values.put("doris.sink.s3.role-arn", "arn:aws:iam::123456789012:role/doris");
        values.put("doris.sink.s3.external-id", "external-id");
        return DorisConfig.fromMap(values, false);
    }

    private static Map<String, String> baseOptions() {
        Map<String, String> values = new HashMap<>();
        values.put("doris.fenodes", "localhost:8030");
        values.put("doris.query.port", "9030");
        values.put("doris.table.identifier", "db.tbl");
        values.put("doris.user", "root");
        values.put("doris.password", "");
        values.put("doris.sink.mode", "tvf");
        values.put("doris.sink.s3.endpoint", "https://s3.example.com");
        values.put("doris.sink.s3.region", "us-east-1");
        values.put("doris.sink.s3.bucket", "staging");
        values.put("doris.sink.s3.prefix", "spark/orders");
        return values;
    }
}
