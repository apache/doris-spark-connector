package org.apache.doris.spark.catalog

import org.apache.spark.sql.connector.catalog.SupportsNamespaces

abstract class DorisTableCatalogBase extends SupportsNamespaces {

  def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = throw new UnsupportedOperationException()

}
