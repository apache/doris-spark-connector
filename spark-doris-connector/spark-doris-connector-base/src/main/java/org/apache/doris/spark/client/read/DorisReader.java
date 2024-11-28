// This code is translated from Scala to Java, using the Apache Doris Spark framework.

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