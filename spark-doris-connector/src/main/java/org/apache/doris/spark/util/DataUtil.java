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

package org.apache.doris.spark.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.Row;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DataUtil {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static final String NULL_VALUE = "\\N";

    public static Object handleColumnValue(Object value) {

        if (value == null) {
            return NULL_VALUE;
        }

        if (value instanceof Date || value instanceof Timestamp) {
            return value.toString();
        }

        if (value instanceof WrappedArray) {

            Object[] arr = JavaConversions.seqAsJavaList((WrappedArray) value).toArray();
            return Arrays.toString(arr);
        }

        return value;

    }

    public static String rowsToCsv(List<Row> rows, String sep, String lineDelimiter) {
        StringBuilder builder = new StringBuilder();
        return rows.stream().map(row -> {
            if (builder.length() != 0) {
                builder.delete(0, builder.length());
            }
            int n = row.size();
            if (n > 0) {
                builder.append(handleColumnValue(row.get(0)));
                int i = 1;
                while (i < n) {
                    builder.append(sep);
                    builder.append(handleColumnValue(row.get(i)));
                    i++;
                }
            }
            return builder.toString();
        }).collect(Collectors.joining(lineDelimiter));
    }

    public static String rowsToJson(List<Row> rows, String[] dfColumns, String lineDelimiter)
            throws JsonProcessingException {

        List<Map<String, Object>> batch = new LinkedList<>();
        for (Row row : rows) {
            Map<String, Object> rowMap = new HashMap<>(row.size());
            for (int i = 0; i < dfColumns.length; i++) {
                rowMap.put(dfColumns[i], handleColumnValue(row.get(i)));
            }
            batch.add(rowMap);
        }

        // when lineDelimiter is null, use strip_outer_array mode, otherwise use json_by_line mode
        if (lineDelimiter == null) {
            return MAPPER.writeValueAsString(batch);
        } else {
            StringBuilder builder = new StringBuilder();
            for (Map<String, Object> data : batch) {
                builder.append(MAPPER.writeValueAsString(data)).append(lineDelimiter);
            }
            int lastIdx = builder.lastIndexOf(lineDelimiter);
            if (lastIdx != -1) {
                return builder.substring(0, lastIdx);
            }
            return builder.toString();
        }
    }

    public static byte[] rowToCsvBytes(Row row, String sep) {
        StringBuilder builder = new StringBuilder();
        int n = row.size();
        if (n > 0) {
            builder.append(handleColumnValue(row.get(0)));
            int i = 1;
            while (i < n) {
                builder.append(sep);
                builder.append(handleColumnValue(row.get(i)));
                i++;
            }
        }
        return builder.toString().getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] rowToJsonBytes(Row row, String[] dfColumns)
            throws JsonProcessingException {
        Map<String, Object> rowMap = new HashMap<>(row.size());
        for (int i = 0; i < dfColumns.length; i++) {
            rowMap.put(dfColumns[i], handleColumnValue(row.get(i)));
        }
        return MAPPER.writeValueAsBytes(rowMap);
    }

}
