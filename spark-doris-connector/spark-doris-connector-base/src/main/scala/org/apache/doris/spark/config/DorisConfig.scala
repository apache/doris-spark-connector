package org.apache.doris.spark.config

import org.apache.doris.spark.exception.OptionRequiredException
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.reflect.ClassTag

class DorisConfig private (var configOptions: CaseInsensitiveMap[String]) extends Serializable {

  private val DORIS_REQUEST_AUTH_USER = "doris.request.auth.user"

  private val DORIS_REQUEST_AUTH_PASSWORD = "doris.request.auth.password"

  private def this(options: Map[String, String]) = {
    this(CaseInsensitiveMap(options))
    configOptions = CaseInsensitiveMap(processOptions(options))
    checkOptions(configOptions)
  }

  private def processOptions(options: Map[String, String]): Map[String, String] = {
    val dottedOptions: Map[String, String] = options.map {
      case (k, v) =>
        if (k.startsWith("sink.properties.") || k.startsWith("doris.sink.properties.")) {
          (k, v)
        } else {
          (k.replace('_', '.'), v)
        }
    }
    val processedOptions: Map[String, String] = dottedOptions.filter(!_._1.startsWith("spark.")).map {
      case (k, v) =>
        if (k.startsWith("doris.")) (k, v)
        else ("doris." + k, v)
    }.map {
      case (DORIS_REQUEST_AUTH_USER, v) => (DorisOptions.DORIS_USER.name, v)
      case (DORIS_REQUEST_AUTH_PASSWORD, v) => (DorisOptions.DORIS_PASSWORD.name, v)
      case (k, v) => (k, v)
    }
    processedOptions
  }

  private def checkOptions(options: Map[String, String]): Unit = {

    // require(options.contains(DorisOptions.DORIS_FENODES.name), s"option [${DorisOptions.DORIS_FENODES.name}] is required")
    if (!options.contains(DorisOptions.DORIS_FENODES.name)) {
      throw new OptionRequiredException(DorisOptions.DORIS_FENODES.name)
    } else {
      val feNodes = options(DorisOptions.DORIS_FENODES.name)
      if (feNodes.isEmpty) {
        throw new IllegalArgumentException(s"option [${DorisOptions.DORIS_FENODES.name}] is empty")
      } else if (!feNodes.split(",").exists(_.contains(":"))) {
        throw new IllegalArgumentException(s"option [${DorisOptions.DORIS_FENODES.name}] is not in correct format, for example: host:port[,host2:port]")
      }
    }
    if (!options.contains(DorisOptions.DORIS_TABLE_IDENTIFIER.name)) {
      throw new OptionRequiredException(DorisOptions.DORIS_TABLE_IDENTIFIER.name)
    } else {
      val tableIdentifier = options(DorisOptions.DORIS_TABLE_IDENTIFIER.name)
      if (tableIdentifier.isEmpty) {
        throw new IllegalArgumentException(s"option [${DorisOptions.DORIS_TABLE_IDENTIFIER.name}] is empty")
      } else if (!tableIdentifier.contains(".")) {
        throw new IllegalArgumentException(s"option [${DorisOptions.DORIS_TABLE_IDENTIFIER.name}] is not in correct format, for example: db.table")
      }
    }
    if (!options.contains(DorisOptions.DORIS_USER.name)) {
      throw new OptionRequiredException(DorisOptions.DORIS_USER.name)
    } else if (options(DorisOptions.DORIS_USER.name).isEmpty) {
      throw new IllegalArgumentException(s"option [${DorisOptions.DORIS_USER.name}] is empty")
    }
    if (!options.contains(DorisOptions.DORIS_PASSWORD.name)) {
      throw new OptionRequiredException(DorisOptions.DORIS_PASSWORD.name)
    }
  }

  def contains(option: ConfigOption[_]): Boolean = {
    configOptions.contains(option.name)
  }

  def getValue[T](option: ConfigOption[T])(implicit classTag: ClassTag[T]): T = {
    (configOptions.contains(option.name), option.defaultValue.nonEmpty) match {
      case (true, _) =>
        val originVal = configOptions(option.name)
        classTag.runtimeClass match {
          case c if c == classOf[Int] => originVal.toInt.asInstanceOf[T]
          case c if c == classOf[Long] => originVal.toLong.asInstanceOf[T]
          case c if c == classOf[Double] => originVal.toDouble.asInstanceOf[T]
          case c if c == classOf[Boolean] => originVal.toBoolean.asInstanceOf[T]
          case c if c == classOf[String] => originVal.asInstanceOf[T]
          case _ => throw new IllegalArgumentException(s"Unsupported config value type: ${classTag.runtimeClass}")
        }
      case (false, true) => option.defaultValue.get
      case _ => throw new OptionRequiredException(option.name)
    }
  }

  def setProperty(option: ConfigOption[_], value: String): Unit = {
    configOptions += option.name -> value
    configOptions = CaseInsensitiveMap(processOptions(configOptions))
    checkOptions(configOptions)
  }

  def getSinkProperties: Map[String, String] = configOptions.filter(_._1.startsWith(DorisOptions.STREAM_LOAD_PROP_PREFIX))
    .map {
      case (k, v) => (k.substring(DorisOptions.STREAM_LOAD_PROP_PREFIX.length), v)
    }

  def merge(map: Map[String, String]): Unit = {
    configOptions ++= map
    configOptions = CaseInsensitiveMap(processOptions(configOptions))
    checkOptions(configOptions)
  }

  def toMap: Map[String, String] = configOptions.toMap

}

object DorisConfig {

  def fromSparkConf(sparkConf: SparkConf): DorisConfig = {
    new DorisConfig(sparkConf.getAll.toMap)
  }

  def fromSparkConf(sparkConf: SparkConf, options: Map[String, String]): DorisConfig = {
    new DorisConfig(sparkConf.getAll.toMap ++ options)
  }

  def fromMap(map: Map[String, String]): DorisConfig = {
    new DorisConfig(map)
  }

  def fromMap(map: java.util.Map[String, String]): DorisConfig = {
    new DorisConfig(map.asScala.toMap)
  }

}
