package org.apache.doris.spark.load;

import org.apache.doris.spark.exception.DorisException;
import org.apache.doris.spark.exception.IllegalArgumentException;
import org.apache.doris.spark.util.DataUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class RowInputStream extends InputStream {

    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    private final Iterator<Row> iterator;

    private final String format;

    private final String seq;

    private final byte[] delim;

    private final String[] columns;

    private boolean isFirst = true;

    private ByteBuffer buffer = ByteBuffer.allocate(0);

    public RowInputStream(Iterator<Row> iterator, String format, String seq, String delim, String[] columns) {
        this.iterator = iterator;
        this.format = format;
        this.seq = seq;
        this.delim = delim.getBytes(DEFAULT_CHARSET);
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
            buffer = ByteBuffer.wrap(rowBytes);
            isFirst = false;
        } else {
            if (delim.length + rowBytes.length <= buffer.capacity()) {
                buffer.clear();
            } else {
                buffer = ByteBuffer.allocate(rowBytes.length + delim.length);
            }
            buffer.put(delim);
            buffer.put(rowBytes);
            buffer.flip();
        }
        return true;
    }

    private byte[] rowToByte(Row row) throws DorisException {

        byte[] bytes;

        switch (format.toLowerCase()) {
            case "csv":
                bytes = DataUtil.rowToCsvBytes(row, seq);
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

}
