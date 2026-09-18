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

package org.apache.doris.spark.client.write.tvf;

import org.apache.doris.spark.config.S3TvfOptions;
import org.apache.doris.spark.config.S3TvfOptionsTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class S3TvfSqlBuilderTest {

    @Test
    public void buildsIamRoleAndGzipProperties() throws Exception {
        S3TvfOptions options = S3TvfOptions.fromConfig(S3TvfOptionsTest.configWithRole());
        S3TvfCommittable committable =
                new S3TvfCommittable(
                        "db",
                        "tbl",
                        "label",
                        Arrays.asList("spark/file.json.gz"),
                        Arrays.asList("id"));

        String sql = new S3TvfSqlBuilder(options).buildInsertSql(committable);

        Assert.assertTrue(sql.contains("'s3.role_arn' = 'arn:aws:iam::123456789012:role/doris'"));
        Assert.assertTrue(sql.contains("'s3.external_id' = 'external-id'"));
        Assert.assertTrue(sql.contains("'compress_type' = 'gz'"));
        Assert.assertFalse(sql.contains("s3.access_key"));
        Assert.assertFalse(sql.contains("s3.secret_key"));
    }
}
