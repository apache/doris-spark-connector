package org.apache.doris.spark.load;

import org.apache.doris.spark.exception.DorisException;
import org.apache.doris.spark.exception.IllegalArgumentException;
import org.apache.doris.spark.exception.ShouldNeverHappenException;
import org.apache.doris.spark.util.DataUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/**
 * InputStream for batch load
 */
public class RecordBatchInputStream extends InputStream {

    public static final Logger LOG = LoggerFactory.getLogger(RecordBatchInputStream.class);

    private static final int DEFAULT_BUF_SIZE = 4096;

    /**
     * Load record batch
     */
    private final RecordBatch recordBatch;

    /**
     * first line flag
     */
    private boolean isFirst = true;

    /**
     * record buffer
     */
    private ByteBuffer buffer = ByteBuffer.allocate(0);

    /**
     * record count has been read
     */
    private int readCount = 0;

    /**
     * streaming mode pass through data without process
     */
    private final boolean passThrough;

    public RecordBatchInputStream(RecordBatch recordBatch, boolean passThrough) {
        this.recordBatch = recordBatch;
        this.passThrough = passThrough;
    }

    @Override
    public int read() throws IOException {
        try {
            if (buffer.remaining() == 0 && endOfBatch()) {
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
            if (buffer.remaining() == 0 && endOfBatch()) {
                return -1; // End of stream
            }
        } catch (DorisException e) {
            throw new IOException(e);
        }
        int bytesRead = Math.min(len, buffer.remaining());
        buffer.get(b, off, bytesRead);
        return bytesRead;
    }

    /**
     * Check if the current batch read is over.
     * If the number of reads is greater than or equal to the batch size or there is no next record, return false,
     * otherwise return true.
     *
     * @return Whether the current batch read is over
     * @throws DorisException
     */
    public boolean endOfBatch() throws DorisException {
        Iterator<InternalRow> iterator = recordBatch.getIterator();
        if (readCount >= recordBatch.getBatchSize() || !iterator.hasNext()) {
            return true;
        }
        readNext(iterator);
        return false;
    }

    /**
     * read next record into buffer
     *
     * @param iterator row iterator
     * @throws DorisException
     */
    private void readNext(Iterator<InternalRow> iterator) throws DorisException {
        if (!iterator.hasNext()) {
            throw new ShouldNeverHappenException();
        }
        byte[] delim = recordBatch.getDelim();
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
        readCount++;
    }

    /**
     * Check if the buffer has enough capacity.
     *
     * @param need required buffer space
     */
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

    /**
     * Calculate new capacity for buffer expansion.
     *
     * @param capacity current buffer capacity
     * @param minCapacity required min buffer space
     * @return new capacity
     */
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

    /**
     * Convert Spark row data to byte array
     *
     * @param row row data
     * @return byte array
     * @throws DorisException
     */
    private byte[] rowToByte(InternalRow row) throws DorisException {

        byte[] bytes;

        if (passThrough) {
            bytes = row.getString(0).getBytes(StandardCharsets.UTF_8);
            return bytes;
        }

        switch (recordBatch.getFormat().toLowerCase()) {
            case "csv":
                bytes = DataUtil.rowToCsvBytes(row, recordBatch.getSchema(), recordBatch.getSep());
                break;
            case "json":
                try {
                    bytes = DataUtil.rowToJsonBytes(row, recordBatch.getSchema());
                } catch (JsonProcessingException e) {
                    throw new DorisException("parse row to json bytes failed", e);
                }
                break;
            default:
                throw new IllegalArgumentException("format", recordBatch.getFormat());
        }

        return bytes;

    }


}
