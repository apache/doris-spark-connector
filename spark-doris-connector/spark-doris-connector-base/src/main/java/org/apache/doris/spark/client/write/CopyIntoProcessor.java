package org.apache.doris.spark.client.write;

import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.util.RowConvertors;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CopyIntoProcessor extends AbstractCopyIntoProcessor<InternalRow> {

    private StructType schema;

    public CopyIntoProcessor(DorisConfig config) throws Exception {
        super(config);
        this.schema = new StructType(new StructField[0]);
    }

    public CopyIntoProcessor(DorisConfig config, StructType schema) throws Exception {
        super(config);
        this.schema = schema;
    }

    @Override
    protected String toFormat(InternalRow row, String format) {
        switch (format) {
            case "csv":
                return RowConvertors.convertToCsv(row, schema, columnSeparator);
            case "json":
                return RowConvertors.convertToJson(row, schema);
            default:
                return null;
        }
    }

    public void setSchema(StructType schema) {
        this.schema = schema;
    }

}
