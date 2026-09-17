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

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class S3TvfCommitterTest {

    @Test
    public void excludesCompressTypeFromSessionVariables() throws Exception {
        Map<String, String> loadProperties = new HashMap<>();
        loadProperties.put("compress_type", "");
        AtomicReference<Map<String, String>> captured = new AtomicReference<>();
        S3TvfLoadClient loadClient = new S3TvfLoadClient() {
            @Override
            public void executeInsert(String sql, Map<String, String> sessionVariables)
                    throws SQLException {
                captured.set(sessionVariables);
            }

            @Override
            public void close() throws IOException {}
        };
        S3TvfOptions options = S3TvfOptions.fromConfig(S3TvfOptionsTest.configWithRole());
        S3TvfCommitter committer = new S3TvfCommitter(options, loadProperties, loadClient);

        committer.commit(new S3TvfCommittable(
                "db", "tbl", "label", Arrays.asList("file.json"), Arrays.asList("id")));

        Assert.assertFalse(captured.get().containsKey("compress_type"));
    }
}
