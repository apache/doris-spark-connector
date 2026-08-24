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

import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Resolves the fixed physical column list used by the TVF write path. */
public final class S3TvfColumnUtils {
    private static final String COLUMNS = "columns";

    private S3TvfColumnUtils() {}

    public static List<String> resolveColumns(
            Map<String, String> loadProperties, StructType schema) {
        String configuredColumns = loadProperties.get(COLUMNS);
        if (configuredColumns == null || configuredColumns.trim().isEmpty()) {
            return Collections.unmodifiableList(Arrays.asList(schema.fieldNames()));
        }

        Set<String> schemaFields = new HashSet<>(Arrays.asList(schema.fieldNames()));
        Set<String> seen = new HashSet<>();
        List<String> columns = new ArrayList<>();
        for (String rawColumn : configuredColumns.split(",", -1)) {
            String column = unquoteIdentifier(rawColumn.trim());
            if (column.isEmpty()) {
                throw new IllegalArgumentException(
                        "doris.sink.properties.columns must not contain an empty column");
            }
            if (!schemaFields.contains(column)) {
                throw new IllegalArgumentException(
                        "Column '"
                                + column
                                + "' from doris.sink.properties.columns is not a Spark input "
                                + "column");
            }
            if (!seen.add(column)) {
                throw new IllegalArgumentException(
                        "Column '"
                                + column
                                + "' is duplicated in doris.sink.properties.columns");
            }
            columns.add(column);
        }
        return Collections.unmodifiableList(columns);
    }

    private static String unquoteIdentifier(String value) {
        if (value.length() >= 2 && value.startsWith("`") && value.endsWith("`")) {
            return value.substring(1, value.length() - 1).replace("``", "`");
        }
        return value;
    }
}
