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

import org.apache.doris.spark.client.DorisFrontendClient;
import org.apache.doris.spark.config.DorisConfig;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

/** JDBC implementation that applies session variables and executes one INSERT. */
public final class JdbcS3TvfLoadClient implements S3TvfLoadClient {
    private static final Pattern SESSION_VARIABLE = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private final DorisFrontendClient frontendClient;

    public JdbcS3TvfLoadClient(DorisConfig config) throws Exception {
        this(new DorisFrontendClient(config));
    }

    JdbcS3TvfLoadClient(DorisFrontendClient frontendClient) {
        this.frontendClient = frontendClient;
    }

    @Override
    public void executeInsert(String sql, Map<String, String> sessionVariables)
            throws SQLException {
        try {
            frontendClient.executeFrontendOnce(connection -> {
                try (Statement statement = connection.createStatement()) {
                    for (Map.Entry<String, String> entry
                            : new TreeMap<>(sessionVariables).entrySet()) {
                        if (!SESSION_VARIABLE.matcher(entry.getKey()).matches()) {
                            throw new SQLException(
                                    "Invalid Doris session variable: " + entry.getKey());
                        }
                        statement.execute(
                                "SET SESSION "
                                        + entry.getKey()
                                        + " = "
                                        + TvfSqlUtils.quoteLiteral(entry.getValue()));
                    }
                    statement.execute(sql);
                }
            });
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("Failed to execute Doris S3 TVF INSERT", e);
        }
    }

    @Override
    public void close() throws IOException {
        frontendClient.close();
    }
}
