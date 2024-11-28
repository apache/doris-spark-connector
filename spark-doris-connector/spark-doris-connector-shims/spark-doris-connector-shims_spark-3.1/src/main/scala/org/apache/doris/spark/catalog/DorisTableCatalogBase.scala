package org.apache.doris.spark.catalog

import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException
import org.apache.spark.sql.connector.catalog.SupportsNamespaces

abstract class DorisTableCatalogBase extends SupportsNamespaces {

  @throws[NoSuchNamespaceException]
  override def dropNamespace(namespace: Array[String]): Boolean = throw new UnsupportedOperationException()

}
