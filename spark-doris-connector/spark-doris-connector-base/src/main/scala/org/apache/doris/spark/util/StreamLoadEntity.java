package org.apache.doris.spark.util;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import org.apache.http.entity.AbstractHttpEntity;

public class StreamLoadEntity extends AbstractHttpEntity {

  private BlockingQueue<byte[]> bufferQueue;

  private volatile boolean closed = false;

  public StreamLoadEntity() {
    this.bufferQueue = new LinkedBlockingDeque<>(1024);
  }

  public StreamLoadEntity(BlockingQueue<byte[]> bufferQueue) {
    this.bufferQueue = bufferQueue;
  }

  @Override
  public boolean isRepeatable() {
    return false;
  }

  @Override
  public long getContentLength() {
    return -1;
  }

  @Override
  public InputStream getContent() throws UnsupportedOperationException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeTo(OutputStream outStream) throws IOException {
    BufferedOutputStream bos = new BufferedOutputStream(outStream);
    while (!closed || bufferQueue.size() > 0) {
      try {
        byte[] input = bufferQueue.poll(1, TimeUnit.SECONDS);
        bos.write(input);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
    bos.flush();
  }

  public void write(byte[] input) {
    try {
      bufferQueue.put(input);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isStreaming() {
    return true;
  }

  public void close() {
    closed = true;
  }
}
