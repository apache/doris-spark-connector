package org.apache.doris.spark.client.write;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.config.DorisOptions;
import org.apache.doris.spark.exception.OptionRequiredException;
import org.apache.doris.spark.util.RowConvertors;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.arrow.ArrowWriter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.ArrowUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

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
    public byte[] toArrowFormat(InternalRow[] rowArray) throws IOException {
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
    public String stringify(InternalRow row, String format) {
        switch (format) {
            case "csv":
                return RowConvertors.convertToCsv(row, schema, columnSeparator);
            case "json":
                return RowConvertors.convertToJson(row, schema);
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



}