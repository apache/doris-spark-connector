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
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
import org.apache.doris.spark.sql.SchemaUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class DataUtil {

    private static final ObjectMapper MAPPER = JsonMapper.builder().addModule(new DefaultScalaModule()).build();

    public static final String NULL_VALUE = "\\N";

    public static byte[] rowToCsvBytes(InternalRow row, StructType schema, String sep) {
        StringBuilder builder = new StringBuilder();
        StructField[] fields = schema.fields();
        int n = row.numFields();
        if (n > 0) {
            builder.append(SchemaUtils.rowColumnValue(row, 0, fields[0].dataType()));
            int i = 1;
            while (i < n) {
                builder.append(sep);
                builder.append(SchemaUtils.rowColumnValue(row, i, fields[i].dataType()));
                i++;
            }
        }
        return builder.toString().getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] rowAddDoubleQuotesToCsvBytes(InternalRow row, StructType schema, String sep) {
        StringBuilder builder = new StringBuilder();
        StructField[] fields = schema.fields();
        int n = row.numFields();
        if (n > 0) {
            builder.append("\"").append(SchemaUtils.rowColumnValue(row, 0, fields[0].dataType())).append("\"");
            int i = 1;
            while (i < n) {
                builder.append(sep);
                builder.append("\"").append(SchemaUtils.rowColumnValue(row, i, fields[i].dataType())).append("\"");
                i++;
            }
        }
        return builder.toString().getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] rowToJsonBytes(InternalRow row, StructType schema) throws JsonProcessingException {
        StructField[] fields = schema.fields();
        Map<String, Object> rowMap = new HashMap<>(row.numFields());
        for (int i = 0; i < fields.length; i++) {
            rowMap.put(fields[i].name(), SchemaUtils.rowColumnValue(row, i, fields[i].dataType()));
        }
        return MAPPER.writeValueAsBytes(rowMap);
    }

}
