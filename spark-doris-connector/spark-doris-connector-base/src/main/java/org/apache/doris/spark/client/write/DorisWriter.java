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

package org.apache.doris.spark.client.write;

import org.apache.doris.spark.config.DorisOptions;

import java.io.IOException;
import java.io.Serializable;

public abstract class DorisWriter<R> implements Serializable {

    protected int batchSize;

    protected int currentBatchCount = 0;

    public DorisWriter(int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException(DorisOptions.DORIS_SINK_BATCH_SIZE.getName() + " must be greater than 0");
        }
        this.batchSize = batchSize;
    }

    public abstract void load(R row) throws Exception;

    public abstract String stop() throws Exception;

    public abstract void close() throws IOException;

    public boolean endOfBatch() {
        return currentBatchCount >= batchSize;
    }

    public int getBatchCount() {
        return currentBatchCount;
    }

    public void resetBatchCount() {
        currentBatchCount = 0;
    }

}