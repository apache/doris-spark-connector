package org.apache.spark.sql

import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, GeneralScalarExpression, LiteralValue}
import org.apache.spark.sql.types.StringType

object ExpressionUtil {

  def buildCoalesceFilter(): Expression = {
    val gse = new GeneralScalarExpression("COALESCE", Array(FieldReference(Seq("A4")), LiteralValue("null", StringType)))
    new Predicate("=", Array(gse, LiteralValue("1", StringType)))
  }

  def buildLowerFilter(): Expression = {
    val gse = new GeneralScalarExpression("LOWER", Array(FieldReference(Seq("A5"))))
    new Predicate("=", Array(gse, LiteralValue("1", StringType)))
  }

}
