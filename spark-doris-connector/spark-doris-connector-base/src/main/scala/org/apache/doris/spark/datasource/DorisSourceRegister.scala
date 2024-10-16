package org.apache.doris.spark.datasource

import org.apache.spark.sql.sources.DataSourceRegister

protected[datasource] trait DorisSourceRegister extends DataSourceRegister {
  override def shortName(): String = "doris"
}
