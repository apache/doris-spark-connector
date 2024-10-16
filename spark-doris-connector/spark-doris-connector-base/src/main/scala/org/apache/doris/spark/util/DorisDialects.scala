package org.apache.doris.spark.util

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.sources.{And, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Not, Or, StringContains, StringEndsWith, StringStartsWith}

import java.sql.{Date, Timestamp}
import java.time.LocalDate

object DorisDialects {

  def compileFilter(filter: Filter): Option[String] = {
    Option[String](filter match {
      case EqualTo(attribute, value) => s"${quote(attribute)} = ${compileValue(value)}"
      case Not(EqualTo(attribute, value)) => s"${quote(attribute)} != ${compileValue(value)}"
      case GreaterThan(attribute, value) => s"${quote(attribute)} > ${compileValue(value)}"
      case GreaterThanOrEqual(attribute, value) => s"${quote(attribute)} >= ${compileValue(value)}"
      case LessThan(attribute, value) => s"${quote(attribute)} < ${compileValue(value)}"
      case LessThanOrEqual(attribute, value) => s"${quote(attribute)} <= ${compileValue(value)}"
      case In(attribute, values) => s"${quote(attribute)} in (${compileValue(values)})"
      case Not(In(attribute, values)) => s"${quote(attribute)} not in (${compileValue(values)})"
      case IsNull(attribute) => s"${quote(attribute)} is null"
      case IsNotNull(attribute) => s"${quote(attribute)} is not null"
      case And(left, right) =>
        val and = Seq(left, right).flatMap(compileFilter)
        if (and.size == 2) {
          and.map(p => s"($p)").mkString(" and ")
        } else null
      case Or(left, right) =>
        val or = Seq(left, right).flatMap(compileFilter)
        if (or.size == 2) {
          or.map(p => s"($p)").mkString(" or ")
        } else null
      case StringContains(attribute, value) => s"${quote(attribute)} like '%$value%'"
      case Not(StringContains(attribute, value)) => s"${quote(attribute)} not like '%$value%'"
      case StringEndsWith(attribute, value) => s"${quote(attribute)} like '%$value'"
      case Not(StringEndsWith(attribute, value)) => s"${quote(attribute)} not like '%$value'"
      case StringStartsWith(attribute, value) => s"${quote(attribute)} like '$value%'"
      case Not(StringStartsWith(attribute, value)) => s"${quote(attribute)} not like '$value%'"
      case _ => null
    })
  }

    /**
     * Converts value to SQL expression.
     *
     * @param value The value to be converted.
     * @return Converted value.
     */
    private def compileValue(value: Any): Any = value match {
      case stringValue: String => s"'${escapeSql(stringValue)}'"
      case timestampValue: Timestamp => "'" + timestampValue + "'"
      case dateValue: Date => "'" + dateValue + "'"
      case dateValue: LocalDate => "'" + dateValue + "'"
      case arrayValue: Array[Any] => arrayValue.map(compileValue).mkString(", ")
      case _ => value
    }

    /**
     * quote column name
     *
     * @param colName column name
     * @return quoted column name
     */
    private def quote(colName: String): String = s"`$colName`"

    private def escapeSql(value: String): String =
      if (value == null) null else StringUtils.replace(value, "'", "''")

}
