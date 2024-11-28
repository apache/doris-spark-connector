package org.apache.doris.spark.sql.sources

import org.apache.spark.sql.sources.DataSourceRegister

trait DorisSourceRegisterTrait extends DataSourceRegister {
  override def shortName(): String = "doris"
}
