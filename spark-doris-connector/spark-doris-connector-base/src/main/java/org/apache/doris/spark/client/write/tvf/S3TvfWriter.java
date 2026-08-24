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
import org.apache.doris.spark.util.RowConvertors;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Writes one logical Spark partition as deterministic JSON Lines objects. */
public final class S3TvfWriter implements AutoCloseable {
    private static final byte NEW_LINE = '\n';

    private final S3TvfOptions options;
    private final String database;
    private final String table;
    private final String normalizedTable;
    private final String labelPrefix;
    private final List<String> columns;
    private final StructType outputSchema;
    private final int[] selectedIndexes;
    private final boolean projectionRequired;
    private final String batchUuid = UUID.randomUUID().toString();
    private final int partitionId;
    private final int batchSize;
    private final S3ObjectStore objectStore;
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    private final List<String> objectKeys = new ArrayList<>();

    private int recordCount;
    private int fileNumber;

    public S3TvfWriter(
            S3TvfOptions options,
            String database,
            String table,
            String labelPrefix,
            StructType inputSchema,
            List<String> columns,
            int partitionId,
            int batchSize,
            S3ObjectStore objectStore) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("S3 TVF batch size must be greater than zero");
        }
        this.options = options;
        this.database = database;
        this.table = table;
        this.normalizedTable = normalizeTable(table);
        this.labelPrefix = labelPrefix;
        this.columns = new ArrayList<>(columns);
        this.selectedIndexes = resolveIndexes(inputSchema, columns);
        this.outputSchema = selectSchema(inputSchema, selectedIndexes);
        this.projectionRequired = requiresProjection(selectedIndexes, inputSchema.fields().length);
        this.partitionId = partitionId;
        this.batchSize = batchSize;
        this.objectStore = objectStore;
    }

    public void write(InternalRow row) throws IOException {
        byte[] json = RowConvertors.convertToJsonBytes(project(row), outputSchema);
        buffer.write(json);
        buffer.write(NEW_LINE);
        recordCount++;
        if (recordCount >= batchSize) {
            uploadBuffer();
        }
    }

    public S3TvfCommittable prepareCommit() throws IOException {
        uploadBuffer();
        return new S3TvfCommittable(database, table, label(), objectKeys, columns);
    }

    private void uploadBuffer() throws IOException {
        if (recordCount == 0) {
            return;
        }
        byte[] content = buffer.toByteArray();
        int currentFileNumber = fileNumber++;
        String fileName = String.format(
                "%s_%s_%s_%d_%d.json",
                labelPrefix,
                normalizedTable,
                batchUuid,
                partitionId,
                currentFileNumber);
        String prefix = options.getPrefix();
        String objectKey = prefix + (prefix.endsWith("/") ? "" : "/") + fileName;
        objectStore.put(objectKey, content);
        objectKeys.add(objectKey);
        buffer.reset();
        recordCount = 0;
    }

    private String label() {
        return labelPrefix
                + "_"
                + normalizedTable
                + "_"
                + batchUuid
                + "_"
                + partitionId;
    }

    private static String normalizeTable(String table) {
        return table.replaceAll("[^A-Za-z0-9_-]", "_");
    }

    private InternalRow project(InternalRow row) {
        if (!projectionRequired) {
            return row;
        }
        Object[] values = new Object[selectedIndexes.length];
        StructField[] fields = outputSchema.fields();
        for (int index = 0; index < selectedIndexes.length; index++) {
            values[index] = row.get(selectedIndexes[index], fields[index].dataType());
        }
        return new GenericInternalRow(values);
    }

    private static int[] resolveIndexes(StructType schema, List<String> columns) {
        int[] indexes = new int[columns.size()];
        for (int index = 0; index < columns.size(); index++) {
            indexes[index] = schema.fieldIndex(columns.get(index));
        }
        return indexes;
    }

    private static StructType selectSchema(StructType schema, int[] indexes) {
        StructField[] selectedFields = new StructField[indexes.length];
        StructField[] fields = schema.fields();
        for (int index = 0; index < indexes.length; index++) {
            selectedFields[index] = fields[indexes[index]];
        }
        return new StructType(selectedFields);
    }

    private static boolean requiresProjection(int[] indexes, int fieldCount) {
        if (indexes.length != fieldCount) {
            return true;
        }
        for (int index = 0; index < indexes.length; index++) {
            if (indexes[index] != index) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        objectStore.close();
    }
}
