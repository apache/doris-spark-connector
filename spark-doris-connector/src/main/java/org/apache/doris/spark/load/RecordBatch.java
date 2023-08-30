package org.apache.doris.spark.load;

import org.apache.spark.sql.Row;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/**
 * Wrapper Object for batch loading
 */
public class RecordBatch {

    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    /**
     * Spark row data iterator
     */
    private final Iterator<Row> iterator;

    /**
     * batch size for single load
     */
    private final int batchSize;

    /**
     * stream load format
     */
    private final String format;

    /**
     * column separator, only used when the format is csv
     */
    private final String sep;

    /**
     * line delimiter
     */
    private final byte[] delim;

    /**
     * column name array, only used when the format is json
     */
    private final String[] columns;

    private RecordBatch(Iterator<Row> iterator, int batchSize, String format, String sep, byte[] delim, String[] columns) {
        this.iterator = iterator;
        this.batchSize = batchSize;
        this.format = format;
        this.sep = sep;
        this.delim = delim;
        this.columns = columns;
    }

    public Iterator<Row> getIterator() {
        return iterator;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public String getFormat() {
        return format;
    }

    public String getSep() {
        return sep;
    }

    public byte[] getDelim() {
        return delim;
    }

    public String[] getColumns() {
        return columns;
    }

    public static Builder newBuilder(Iterator<Row> iterator) {
        return new Builder(iterator);
    }

    /**
     * RecordBatch Builder
     */
    public static class Builder {

        private final Iterator<Row> iterator;

        private int batchSize;

        private String format;

        private String sep;

        private byte[] delim;

        private String[] columns;

        public Builder(Iterator<Row> iterator) {
            this.iterator = iterator;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder format(String format) {
            this.format = format;
            return this;
        }

        public Builder sep(String sep) {
            this.sep = sep;
            return this;
        }

        public Builder delim(String delim) {
            this.delim = delim.getBytes(DEFAULT_CHARSET);
            return this;
        }

        public Builder columns(String[] columns) {
            this.columns = columns;
            return this;
        }

        public RecordBatch build() {
            return new RecordBatch(iterator, batchSize, format, sep, delim, columns);
        }

    }

}
