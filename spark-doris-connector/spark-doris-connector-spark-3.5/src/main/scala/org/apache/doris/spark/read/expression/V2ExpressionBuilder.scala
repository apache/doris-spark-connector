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

package org.apache.doris.spark.read.expression

import org.apache.spark.sql.connector.expressions.filter.{AlwaysFalse, AlwaysTrue, And, Not, Or}
import org.apache.spark.sql.connector.expressions.{Expression, GeneralScalarExpression, Literal, NamedReference}

class V2ExpressionBuilder(inValueLengthLimit: Int) {

  def build(predicate: Expression): String = {
    predicate match {
      case and: And => s"(${build(and.left())} AND ${build(and.right())})"
      case or: Or => s"(${build(or.left())} OR ${build(or.right())})"
      case not: Not =>
        not.child().name() match {
          case "IS_NULL" => build(new GeneralScalarExpression("IS_NOT_NULL", not.children()(0).children()))
          case "=" => build(new GeneralScalarExpression("!=", not.children()(0).children()))
          case _ => s"NOT (${build(not.child())})"
        }
      case _: AlwaysTrue => "1=1"
      case _: AlwaysFalse => "1=0"
      case expr: Expression =>
        expr match {
          case literal: Literal[_] => literal.toString
          case namedRef: NamedReference => namedRef.toString
          case e: GeneralScalarExpression => e.name() match {
            case "IN" =>
              val expressions = e.children()
              if (expressions.nonEmpty && expressions.length <= inValueLengthLimit) {
                s"""`${build(expressions(0))}` IN (${expressions.slice(1, expressions.length).map(build).mkString(",")})"""
              } else null
            case "IS_NULL" => s"`${build(e.children()(0))}` IS NULL"
            case "IS_NOT_NULL" => s"`${build(e.children()(0))}` IS NOT NULL"
            case "STARTS_WITH" => visitStartWith(build(e.children()(0)), build(e.children()(1)));
            case "ENDS_WITH" => visitEndWith(build(e.children()(0)), build(e.children()(1)));
            case "CONTAINS" => visitContains(build(e.children()(0)), build(e.children()(1)));
            case "=" => s"`${build(e.children()(0))}` = ${build(e.children()(1))}"
            case "!=" | "<>" => s"`${build(e.children()(0))}` != ${build(e.children()(1))}"
            case "<" => s"`${build(e.children()(0))}` < ${build(e.children()(1))}"
            case "<=" => s"`${build(e.children()(0))}` <= ${build(e.children()(1))}"
            case ">" => s"`${build(e.children()(0))}` > ${build(e.children()(1))}"
            case ">=" => s"`${build(e.children()(0))}` >= ${build(e.children()(1))}"
            case _ => null
          }
        }
    }
  }

  def visitStartWith(l: String, r: String): String = {
    val value = r.substring(1, r.length - 1)
    s"`$l` LIKE '$value%'"
  }

  def visitEndWith(l: String, r: String): String = {
    val value = r.substring(1, r.length - 1)
    s"`$l` LIKE '%$value'"
  }

  def visitContains(l: String, r: String): String = {
    val value = r.substring(1, r.length - 1)
    s"`$l` LIKE '%$value%'"
  }

}
