package org.apache.doris.spark.catalog

import org.apache.doris.spark.client.DorisFrontend
import org.apache.doris.spark.config.{DorisConfig, DorisOptions}
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException
import org.apache.spark.sql.connector.catalog.{Identifier, NamespaceChange, SupportsNamespaces, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

class DorisTableCatalog extends TableCatalog with SupportsNamespaces {

  private var catalogName: Option[String] = None

  private var dorisConfig: DorisConfig = _

  private var frontend: DorisFrontend = _

  override def name(): String = {
    require(catalogName.nonEmpty, "The Doris table catalog is not initialed")
    catalogName.get
  }

  override def initialize(name: String, caseInsensitiveStringMap: CaseInsensitiveStringMap): Unit = {
    assert(catalogName.isEmpty, "The Doris table catalog is already initialed")
    catalogName = Some(name)
    dorisConfig = DorisConfig.fromMap(caseInsensitiveStringMap.asScala.toMap)
    frontend = DorisFrontend(dorisConfig)
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    DorisFrontend.listTables(namespace).map(i => Identifier.of(i._1, i._2))
  }

  override def loadTable(identifier: Identifier): Table = {
    checkIdentifier(identifier)
    new DorisTable(identifier, DorisConfig.fromMap(dorisConfig.configOptions.toMap +
      (DorisOptions.DORIS_TABLE_IDENTIFIER.name -> getFullTableName(identifier))), None)
  }

  override def createTable(identifier: Identifier, structType: StructType, transforms: Array[Transform], map: util.Map[String, String]): Table = throw new UnsupportedOperationException()

  override def alterTable(identifier: Identifier, tableChanges: TableChange*): Table = throw new UnsupportedOperationException()

  override def dropTable(identifier: Identifier): Boolean = throw new UnsupportedOperationException()

  override def renameTable(identifier: Identifier, identifier1: Identifier): Unit = throw new UnsupportedOperationException()

  override def listNamespaces(): Array[Array[String]] = {
    DorisFrontend.listDatabases().map(Array(_))
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    namespace match {
      case Array() => listNamespaces()
      case Array(_) if DorisFrontend.databaseExists(namespace(0)) => Array()
      case _ => throw new NoSuchNamespaceException(namespace)
    }
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    namespace match {
      case Array(database) =>
        if (!DorisFrontend.databaseExists(database)) {
          throw new NoSuchNamespaceException(namespace)
        }
        new util.HashMap[String, String]()
      case _ => throw new NoSuchNamespaceException(namespace)
    }
  }

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = throw new UnsupportedOperationException()

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = throw new UnsupportedOperationException()

  override def dropNamespace(namespace: Array[String]): Boolean = throw new UnsupportedOperationException()

  private def checkIdentifier(identifier: Identifier): Unit = {
    if (identifier.namespace().length > 1) {
      throw new NoSuchNamespaceException(identifier.namespace())
    }
  }

  private def getFullTableName(identifier: Identifier): String = {
    (identifier.namespace() :+ identifier.name()).map(item => s"""`$item`""").mkString(".")
  }

}
