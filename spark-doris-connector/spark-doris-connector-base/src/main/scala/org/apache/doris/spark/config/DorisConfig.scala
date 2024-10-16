package org.apache.doris.spark.config

import org.apache.doris.spark.exception.ConfigurationNotSetException
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.reflect.ClassTag

class DorisConfig(var configOptions: CaseInsensitiveMap[String]) extends Serializable {

  private val SINK_PROPERTIES_PREFIX = "doris.sink.properties."

  private def this(options: Map[String, String]) = this(CaseInsensitiveMap(options))

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
      case _ => throw new ConfigurationNotSetException(option.name)
    }
  }

  def setProperty(option: ConfigOption[_], value: String): Unit = {
    configOptions += option.name -> value
  }

  def getSinkProperties: Map[String, String] = configOptions.filter(_._1.startsWith(SINK_PROPERTIES_PREFIX))
    .map {
      case (k, v) => (k.substring(SINK_PROPERTIES_PREFIX.length), v)
    }

  def merge(map: Map[String, String]): Unit = {
    configOptions ++= map
  }

}

object DorisConfig {

  def fromSparkConf(sparkConf: SparkConf): DorisConfig = {
    new DorisConfig(sparkConf.getAll.toMap)
  }

  def fromMap(map: Map[String, String]): DorisConfig = {
    new DorisConfig(map)
  }

  def fromMap(map: java.util.Map[String, String]): DorisConfig = {
    new DorisConfig(map.asScala.toMap)
  }

}
