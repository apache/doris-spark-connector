// This code is related to Apache Doris and Spark.
package org.apache.doris.spark.client.write;

import java.io.Serializable;

public interface DorisCommitter extends Serializable {

    void commit(String message) throws Exception;

    void abort(String message) throws Exception;
}