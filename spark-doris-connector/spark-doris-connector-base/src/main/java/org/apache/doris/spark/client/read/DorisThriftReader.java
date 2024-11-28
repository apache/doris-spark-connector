// Translated from Scala to Java for Apache Doris Spark client
package org.apache.doris.spark.client.read;

import org.apache.doris.spark.client.entity.DorisReaderPartition;
import org.apache.doris.spark.rest.models.Schema;
import org.apache.doris.spark.util.SchemaConvertors;
import scala.collection.JavaConverters;

public class DorisThriftReader extends AbstractThriftReader {

    public DorisThriftReader(DorisReaderPartition partition) throws Exception {
        super(partition);
    }

    @Override
    protected Schema getDorisSchema() {
        return SchemaConvertors.convertToSchema(
                JavaConverters.asScalaBufferConverter(scanOpenResult.getSelectedColumns()).asScala().toSeq());
    }

}