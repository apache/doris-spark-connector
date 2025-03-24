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
package org.apache.doris.spark.load;

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
    this(1000);
  }

  public StreamLoadEntity(int bufferSize) {
    this.bufferQueue = new LinkedBlockingDeque<>(bufferSize);
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
        if (input != null) {
          bos.write(input);
        }
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
