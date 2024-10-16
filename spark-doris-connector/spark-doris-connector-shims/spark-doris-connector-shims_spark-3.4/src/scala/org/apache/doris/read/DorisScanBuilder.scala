package org.apache.doris.read

import org.apache.doris.spark.config.DorisConfig
import org.apache.spark.sql.types.StructType
import read.DorisScanBuilderTrait

class DorisScanBuilder(config: DorisConfig, schema: StructType) extends DorisScanBuilderTrait(config, schema) {}
