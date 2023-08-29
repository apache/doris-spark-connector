package org.apache.doris.spark.load;

import org.apache.doris.spark.exception.DorisException;
import org.apache.doris.spark.exception.IllegalArgumentException;
import org.apache.doris.spark.util.DataUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class RowInputStream extends InputStream {

    public static final Logger LOG = LoggerFactory.getLogger(RowInputStream.class);

    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    private static final int DEFAULT_BUF_SIZE = 4096;

    private final Iterator<Row> iterator;

    private final String format;

    private final String sep;

    private final byte[] delim;

    private final String[] columns;

    private boolean isFirst = true;

    private ByteBuffer buffer = ByteBuffer.allocate(0);

    private RowInputStream(Iterator<Row> iterator, String format, String sep, byte[] delim, String[] columns) {
        this.iterator = iterator;
        this.format = format;
        this.sep = sep;
        this.delim = delim;
        this.columns = columns;
    }

    @Override
    public int read() throws IOException {
        try {
            if (buffer.remaining() == 0 && !readNext()) {
                return -1; // End of stream
            }
        } catch (DorisException e) {
            throw new IOException(e);
        }
        return buffer.get() & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        try {
            if (buffer.remaining() == 0 && !readNext()) {
                return -1; // End of stream
            }
        } catch (DorisException e) {
            throw new IOException(e);
        }
        int bytesRead = Math.min(len, buffer.remaining());
        buffer.get(b, off, bytesRead);
        return bytesRead;
    }

    public boolean readNext() throws DorisException {
        if (!iterator.hasNext()) {
            return false;
        }
        byte[] rowBytes = rowToByte(iterator.next());
        if (isFirst) {
            ensureCapacity(rowBytes.length);
            buffer.put(rowBytes);
            buffer.flip();
            isFirst = false;
        } else {
            ensureCapacity(delim.length + rowBytes.length);
            buffer.put(delim);
            buffer.put(rowBytes);
            buffer.flip();
        }
        return true;
    }

    private void ensureCapacity(int need) {

        int capacity = buffer.capacity();

        if (need <= capacity) {
            buffer.clear();
            return;
        }

        // need to extend
        int newCapacity = calculateNewCapacity(capacity, need);
        LOG.info("expand buffer, min cap: {}, now cap: {}, new cap: {}", need, capacity, newCapacity);
        buffer = ByteBuffer.allocate(newCapacity);

    }

    private int calculateNewCapacity(int capacity, int minCapacity) {
        int newCapacity;
        if (capacity == 0) {
            newCapacity = DEFAULT_BUF_SIZE;
            while (newCapacity < minCapacity) {
                newCapacity = newCapacity << 1;
            }
        } else {
            newCapacity = capacity << 1;
        }
        return newCapacity;
    }

    private byte[] rowToByte(Row row) throws DorisException {

        byte[] bytes;

        switch (format.toLowerCase()) {
            case "csv":
                bytes = DataUtil.rowToCsvBytes(row, sep);
                break;
            case "json":
                try {
                    bytes = DataUtil.rowToJsonBytes(row, columns);
                } catch (JsonProcessingException e) {
                    throw new DorisException("parse row to json bytes failed", e);
                }
                break;
            default:
                throw new IllegalArgumentException("format", format);
        }

        return bytes;

    }

    public static Builder newBuilder(Iterator<Row> rows) {
        return new Builder(rows);
    }

    public static class Builder {

        private final Iterator<Row> rows;

        private String format;

        private String sep;

        private byte[] delim;

        private String[] columns;

        private Builder(Iterator<Row> rows) {
            this.rows = rows;
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

        public RowInputStream build() {
            return new RowInputStream(rows, format, sep, delim, columns);
        }

    }


}
