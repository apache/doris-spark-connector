package org.apache.doris.spark.load;

import org.apache.doris.spark.exception.DorisException;
import org.apache.doris.spark.exception.IllegalArgumentException;
import org.apache.doris.spark.util.DataUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class RowInputStream extends InputStream {

    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    private final Iterator<Row> iterator;

    private final String format;

    private final String seq;

    private final String delim;

    private final String[] columns;

    private CharBuffer current;

    private ByteBuffer pending;

    public RowInputStream(Iterator<Row> iterator, String format, String seq, String delim, String[] columns) {
        this.iterator = iterator;
        this.format = format;
        this.seq = seq;
        this.delim = delim;
        this.columns = columns;
    }

    @Override
    public int read() throws IOException {
        for(;;) {
            if(pending != null && pending.hasRemaining())
                return pending.get() & 0xff;
            if(!ensureCurrent()) return -1;
            if(pending == null) pending = ByteBuffer.allocate(4096);
            else pending.compact();
            DEFAULT_CHARSET.encode(current);
            pending.flip();
        }
    }

    private boolean ensureCurrent() {
        while(current == null || !current.hasRemaining()) {
            if(!iterator.hasNext()) return false;
            current = CharBuffer.wrap(iterator.next().toString());
        }
        return true;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int transferred = 0;
        if(pending != null && pending.hasRemaining()) {
            boolean serveByBuffer = pending.remaining() >= len;
            pending.get(b, off, transferred = Math.min(pending.remaining(), len));
            if(serveByBuffer) return transferred;
            len -= transferred;
            off += transferred;
        }
        ByteBuffer bb = ByteBuffer.wrap(b, off, len);
        while(bb.hasRemaining() && ensureCurrent()) {
            int r = bb.remaining();
            try {
                bb.put(rowToByte(iterator.next()));
            } catch (DorisException e) {
                throw new IOException(e);
            }
            transferred += r - bb.remaining();
        }
        return transferred == 0? -1: transferred;
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
