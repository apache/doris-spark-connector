package org.apache.doris.spark.read

import org.apache.doris.spark.config.DorisConfig
import org.apache.doris.spark.read.DorisScanBuilderTrait
import org.apache.spark.sql.types.StructType

class DorisScanBuilder(config: DorisConfig, schema: StructType) extends DorisScanBuilderTrait(config, schema) {}