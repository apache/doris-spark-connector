package org.apache.doris.spark.writer

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.doris.spark.cfg.SparkSettings
import org.apache.spark.sql.Row

import java.nio.charset.StandardCharsets
import java.sql.Timestamp

class RowSerializer(format: String,
                    columns: Array[String],
                    columnSeparator: String
                   ) {

  private val mapper = new ObjectMapper();

  def serialize(row: Row): Array[Byte] = {
    if (columns.length != row.size)
      return Array.empty[Byte]
    format.toUpperCase match {
      case "CSV" => toCsv(row)
      case "JSON" => toJson(row)
      case _ => throw new IllegalArgumentException("")
    }

  }

  private def toCsv(row: Row): Array[Byte] = {
    (0 to columns.length).map(i => row.get(i).toString).mkString(columnSeparator).getBytes(StandardCharsets.UTF_8)
  }

  private def toJson(row: Row): Array[Byte] = {
    mapper.writeValueAsBytes((0 to columns.length).map(i => {
      val value = row.get(i)
      if (value.isInstanceOf[Timestamp]) {
        (columns(i), value.toString)
      } else {
        (columns(i), value)
      }
    }).toMap)
  }

}

object RowSerializer {

  def apply(settings: SparkSettings, columns: Array[String]): RowSerializer = {
    val format = settings.getProperty("format", "csv")
    val columnSeparator = escapeString(settings.getProperty("column_separator", "\t"))
    new RowSerializer(format, columns, columnSeparator)
  }

  private def escapeString(hexData: String): String = {
    if (hexData.startsWith("\\x") || hexData.startsWith("\\X")) {
      try {
        val data = hexData.substring(2)
        val stringBuilder = new StringBuilder
        var i = 0
        while (i < data.length) {
          val hexByte = data.substring(i, i + 2)
          val decimal = Integer.parseInt(hexByte, 16)
          val character = decimal.toChar
          stringBuilder.append(character)

          i += 2
        }
        return stringBuilder.toString
      } catch {
        case e: Exception =>
          throw new RuntimeException("escape column_separator or line_delimiter error.{}", e)
      }
    }
    hexData
  }

}
