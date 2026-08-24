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

import org.apache.doris.spark.config.S3TvfOptions;

import java.util.List;
import java.util.StringJoiner;

/** Builds one S3 TVF INSERT for one Spark partition. */
public final class S3TvfSqlBuilder {
    private final S3TvfOptions options;

    public S3TvfSqlBuilder(S3TvfOptions options) {
        this.options = options;
    }

    public String buildInsertSql(S3TvfCommittable committable) {
        List<String> objectKeys = committable.getObjectKeys();
        List<String> columns = committable.getColumns();
        if (objectKeys.isEmpty()) {
            throw new IllegalArgumentException("S3 TVF object keys must not be empty");
        }
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("S3 TVF columns must not be empty");
        }
        String columnSql = joinIdentifiers(columns);
        String uri = buildUri(objectKeys);
        return "INSERT INTO "
                + TvfSqlUtils.quoteIdentifier(committable.getDatabase())
                + "."
                + TvfSqlUtils.quoteIdentifier(committable.getTable())
                + " WITH LABEL "
                + TvfSqlUtils.quoteIdentifier(committable.getLabel())
                + " ("
                + columnSql
                + ") SELECT "
                + columnSql
                + " FROM S3("
                + property("uri", uri)
                + ","
                + property("format", "json")
                + ","
                + property("read_json_by_line", "true")
                + ","
                + property("s3.endpoint", options.getEndpoint())
                + ","
                + property("s3.region", options.getRegion())
                + ","
                + property("s3.access_key", options.getAccessKey())
                + ","
                + property("s3.secret_key", options.getSecretKey())
                + ","
                + property("use_path_style", Boolean.toString(options.isPathStyleAccess()))
                + ")";
    }

    private String buildUri(List<String> objectKeys) {
        if (objectKeys.size() == 1) {
            return "s3://" + options.getBucket() + "/" + objectKeys.get(0);
        }
        StringJoiner keys = new StringJoiner(",");
        for (String objectKey : objectKeys) {
            keys.add(objectKey);
        }
        return "s3://" + options.getBucket() + "/{" + keys + "}";
    }

    private static String joinIdentifiers(List<String> columns) {
        StringJoiner joiner = new StringJoiner(",");
        for (String column : columns) {
            joiner.add(TvfSqlUtils.quoteIdentifier(column));
        }
        return joiner.toString();
    }

    private static String property(String key, String value) {
        return TvfSqlUtils.quoteLiteral(key) + " = " + TvfSqlUtils.quoteLiteral(value);
    }
}
