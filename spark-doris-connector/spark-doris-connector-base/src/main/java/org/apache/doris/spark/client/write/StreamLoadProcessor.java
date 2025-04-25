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
package org.apache.doris.spark.client.write;

import java.nio.charset.StandardCharsets;
import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.config.DorisOptions;
import org.apache.doris.spark.exception.OptionRequiredException;
import org.apache.doris.spark.rest.models.DataFormat;
import org.apache.doris.spark.util.RowConvertors;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.arrow.ArrowWriter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.ArrowUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

public class StreamLoadProcessor extends AbstractStreamLoadProcessor<InternalRow> {

    private StructType schema;

    public StreamLoadProcessor(DorisConfig config) throws Exception {
        super(config);
        this.schema = new StructType(new StructField[0]);
    }

    public StreamLoadProcessor(DorisConfig config, StructType schema) throws Exception {
        super(config);
        this.schema = schema;
    }

    @Override
    public byte[] toArrowFormat(List<InternalRow> rowArray) throws IOException {
        Schema arrowSchema = ArrowUtils.toArrowSchema(schema, "UTC");
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, new RootAllocator(Integer.MAX_VALUE));
        ArrowWriter arrowWriter = ArrowWriter.create(root);
        for (InternalRow row : rowArray) {
            arrowWriter.write(row);
        }
        arrowWriter.finish();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowStreamWriter writer = new ArrowStreamWriter(root, new DictionaryProvider.MapDictionaryProvider(), out);
        writer.writeBatch();
        writer.end();
        return out.toByteArray();
    }

    @Override
    protected String getPassThroughData(InternalRow row) {
        return row.getString(0);
    }

    @Override
    public byte[] stringify(InternalRow row, DataFormat format) {
        switch (format) {
            case CSV:
                return RowConvertors.convertToCsv(row, schema, columnSeparator).getBytes(
                    StandardCharsets.UTF_8);
            case JSON:
                return RowConvertors.convertToJsonBytes(row, schema);
            default:
                return null;
        }
    }

    @Override
    public String getWriteFields() throws OptionRequiredException {
        if (config.contains(DorisOptions.DORIS_WRITE_FIELDS)) {
            return config.getValue(DorisOptions.DORIS_WRITE_FIELDS);
        } else {
            StringBuilder fields = new StringBuilder();
            for (StructField field : schema.fields()) {
                if (fields.length() > 0) {
                    fields.append(",");
                }
                fields.append("`").append(field.name()).append("`");
            }
            return fields.toString();
        }
    }

    @Override
    protected String generateStreamLoadLabel() throws OptionRequiredException {
        TaskContext taskContext = TaskContext.get();
        int stageId = taskContext.stageId();
        long taskAttemptId = taskContext.taskAttemptId();
        int partitionId = taskContext.partitionId();
        String prefix = config.getValue(DorisOptions.DORIS_SINK_LABEL_PREFIX);
        return String.format("%s-%d-%d-%d-%d", prefix, stageId, taskAttemptId, partitionId, System.currentTimeMillis());
    }

    public void setSchema(StructType schema) {
        this.schema = schema;
    }

    @Override
    protected InternalRow copy(InternalRow row) {
        return row.copy();
    }
}