// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.spark.catalog

import org.apache.doris.spark.client.DorisFrontendClient
import org.apache.doris.spark.config.{DorisConfig, DorisOptions}
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException
import org.apache.spark.sql.connector.catalog.{Identifier, NamespaceChange, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

class DorisTableCatalog extends DorisTableCatalogBase with TableCatalog {

  private var catalogName: Option[String] = None

  private var dorisConfig: DorisConfig = _

  private var frontend: DorisFrontendClient = _

  override def name(): String = {
    require(catalogName.nonEmpty, "The Doris table catalog is not initialed")
    catalogName.get
  }

  override def initialize(name: String, caseInsensitiveStringMap: CaseInsensitiveStringMap): Unit = {
    assert(catalogName.isEmpty, "The Doris table catalog is already initialed")
    catalogName = Some(name)
    dorisConfig = DorisConfig.fromMap(caseInsensitiveStringMap)
    frontend = new DorisFrontendClient(dorisConfig)
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    frontend.listTables(namespace).asScala.map(i => Identifier.of(i.getLeft, i.getValue)).toArray
  }

  override def loadTable(identifier: Identifier): Table = {
    checkIdentifier(identifier)
    new DorisTable(identifier, DorisConfig.fromMap((dorisConfig.toMap.asScala +
      (DorisOptions.DORIS_TABLE_IDENTIFIER.getName -> getFullTableName(identifier))).asJava), None)
  }

  override def createTable(identifier: Identifier, structType: StructType, transforms: Array[Transform], map: util.Map[String, String]): Table = throw new UnsupportedOperationException()

  override def alterTable(identifier: Identifier, tableChanges: TableChange*): Table = throw new UnsupportedOperationException()

  override def dropTable(identifier: Identifier): Boolean = throw new UnsupportedOperationException()

  override def renameTable(identifier: Identifier, identifier1: Identifier): Unit = throw new UnsupportedOperationException()

  override def listNamespaces(): Array[Array[String]] = {
    frontend.listDatabases().map(Array(_))
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    namespace match {
      case Array() => listNamespaces()
      case Array(_) if frontend.databaseExists(namespace(0)) => Array()
      case _ => throw new NoSuchNamespaceException(namespace)
    }
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    namespace match {
      case Array(database) =>
        if (!frontend.databaseExists(database)) {
          throw new NoSuchNamespaceException(namespace)
        }
        new util.HashMap[String, String]()
      case _ => throw new NoSuchNamespaceException(namespace)
    }
  }

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = throw new UnsupportedOperationException()

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = throw new UnsupportedOperationException()

  private def checkIdentifier(identifier: Identifier): Unit = {
    if (identifier.namespace().length > 1) {
      throw new NoSuchNamespaceException(identifier.namespace())
    }
  }

  private def getFullTableName(identifier: Identifier): String = {
    (identifier.namespace() :+ identifier.name()).map(item => s"""`$item`""").mkString(".")
  }

}
