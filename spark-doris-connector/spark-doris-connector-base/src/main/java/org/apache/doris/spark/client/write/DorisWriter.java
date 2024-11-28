// This code is related to an Apache Spark client for Doris.
package org.apache.doris.spark.client.write;

import java.io.IOException;
import java.io.Serializable;

public interface DorisWriter<R> extends Serializable {

    void load(R row) throws Exception;

    String stop() throws Exception;

    void close() throws IOException;
}