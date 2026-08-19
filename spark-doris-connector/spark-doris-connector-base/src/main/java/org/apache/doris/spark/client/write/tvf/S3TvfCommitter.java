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

package org.apache.doris.spark.client.write.tvf;

import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.config.S3TvfOptions;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/** Commits one Spark partition with one Doris INSERT. */
public final class S3TvfCommitter implements AutoCloseable {
    private static final String COLUMNS = "columns";
    private static final String PARTIAL_COLUMNS = "partial_columns";
    private static final String FORMAT = "format";
    private static final String READ_JSON_BY_LINE = "read_json_by_line";
    private static final String ENABLE_UNIQUE_KEY_PARTIAL_UPDATE =
            "enable_unique_key_partial_update";

    private final Map<String, String> sessionVariables;
    private final S3TvfLoadClient loadClient;
    private final S3TvfSqlBuilder sqlBuilder;

    public S3TvfCommitter(DorisConfig config) throws Exception {
        this(
                S3TvfOptions.fromConfig(config),
                config.getSinkProperties(),
                new JdbcS3TvfLoadClient(config));
    }

    S3TvfCommitter(
            S3TvfOptions options,
            Map<String, String> loadProperties,
            S3TvfLoadClient loadClient) {
        this.sessionVariables = toSessionVariables(loadProperties);
        this.loadClient = loadClient;
        this.sqlBuilder = new S3TvfSqlBuilder(options);
    }

    public void commit(S3TvfCommittable committable) throws IOException {
        if (committable.isEmpty()) {
            return;
        }
        try {
            loadClient.executeInsert(
                    sqlBuilder.buildInsertSql(committable),
                    sessionVariables);
        } catch (SQLException e) {
            throw new IOException(
                    "Doris INSERT failed for S3 TVF label " + committable.getLabel(), e);
        }
    }

    private static Map<String, String> toSessionVariables(Map<String, String> loadProperties) {
        Map<String, String> values = new TreeMap<>();
        for (Map.Entry<String, String> entry : loadProperties.entrySet()) {
            String name = entry.getKey();
            if (!COLUMNS.equals(name)
                    && !PARTIAL_COLUMNS.equals(name)
                    && !FORMAT.equals(name)
                    && !READ_JSON_BY_LINE.equals(name)) {
                values.put(name, entry.getValue());
            }
        }
        if (loadProperties.containsKey(PARTIAL_COLUMNS)) {
            values.put(
                    ENABLE_UNIQUE_KEY_PARTIAL_UPDATE, loadProperties.get(PARTIAL_COLUMNS));
        }
        return new LinkedHashMap<>(values);
    }

    @Override
    public void close() throws IOException {
        loadClient.close();
    }
}
