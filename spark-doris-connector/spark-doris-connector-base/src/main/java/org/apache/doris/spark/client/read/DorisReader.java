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

package org.apache.doris.spark.client.read;

import org.apache.doris.spark.client.entity.DorisReaderPartition;
import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.exception.DorisException;

public abstract class DorisReader {

    protected DorisReaderPartition partition;
    protected DorisConfig config;
    protected RowBatch rowBatch;

    public DorisReader(DorisReaderPartition partition) {
        this.partition = partition;
        this.config = partition.getConfig();
    }

    public abstract boolean hasNext() throws DorisException;

    public abstract Object next() throws DorisException;

    public abstract void close();
}