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

package org.apache.doris.spark.sql

import org.apache.doris.spark.config.DorisOptions
import org.apache.doris.spark.exception.DorisException
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources._
import org.hamcrest.core.StringStartsWith.startsWith
import org.junit._
import org.slf4j.LoggerFactory

class TestUtils extends ExpectedExceptionTest {
  private lazy val logger = LoggerFactory.getLogger(classOf[TestUtils])

  @Test
  def testCompileFilter(): Unit = {
    val dialect = JdbcDialects.get("")
    val inValueLengthLimit = 5

    val equalFilter = EqualTo("left", 5)
    val notEqualFilter = Not(EqualTo("left", 5))
    val greaterThanFilter = GreaterThan("left", 5)
    val greaterThanOrEqualFilter = GreaterThanOrEqual("left", 5)
    val lessThanFilter = LessThan("left", 5)
    val lessThanOrEqualFilter = LessThanOrEqual("left", 5)
    val validInFilter = In("left", Array(1, 2, 3, 4))
    val emptyInFilter = In("left", Array.empty)
    val invalidInFilter = In("left", Array(1, 2, 3, 4, 5))
    val notInFilter = Not(In("left", Array(1, 2, 3)))
    val isNullFilter = IsNull("left")
    val isNotNullFilter = IsNotNull("left")
    val validAndFilter = And(equalFilter, greaterThanFilter)
    val invalidAndFilter = And(equalFilter, invalidInFilter)
    val validOrFilter = Or(equalFilter, greaterThanFilter)
    val invalidOrFilter = Or(equalFilter, invalidInFilter)
    val stringContainsFilter = StringContains("left", "right")
    val notStringContainsFilter = Not(StringContains("left", "right"))
    val stringEndsWithFilter = StringEndsWith("left", "right")
    val notStringEndsWithFilter = Not(StringEndsWith("left", "right"))
    val stringStartsWithFilter = StringStartsWith("left", "right")
    val notStringStartsWithFilter = Not(StringStartsWith("left", "right"))

    Assert.assertEquals("`left` = 5", Utils.compileFilter(equalFilter, dialect, inValueLengthLimit).get)
    Assert.assertEquals("`left` != 5", Utils.compileFilter(notEqualFilter, dialect, inValueLengthLimit).get)
    Assert.assertEquals("`left` > 5", Utils.compileFilter(greaterThanFilter, dialect, inValueLengthLimit).get)
    Assert.assertEquals("`left` >= 5", Utils.compileFilter(greaterThanOrEqualFilter, dialect, inValueLengthLimit).get)
    Assert.assertEquals("`left` < 5", Utils.compileFilter(lessThanFilter, dialect, inValueLengthLimit).get)
    Assert.assertEquals("`left` <= 5", Utils.compileFilter(lessThanOrEqualFilter, dialect, inValueLengthLimit).get)
    Assert.assertEquals("`left` in (1, 2, 3, 4)", Utils.compileFilter(validInFilter, dialect, inValueLengthLimit).get)
    Assert.assertTrue(Utils.compileFilter(emptyInFilter, dialect, inValueLengthLimit).isEmpty)
    Assert.assertTrue(Utils.compileFilter(invalidInFilter, dialect, inValueLengthLimit).isEmpty)
    Assert.assertEquals("`left` not in (1, 2, 3)", Utils.compileFilter(notInFilter, dialect, inValueLengthLimit).get)
    Assert.assertEquals("`left` is null", Utils.compileFilter(isNullFilter, dialect, inValueLengthLimit).get)
    Assert.assertEquals("`left` is not null", Utils.compileFilter(isNotNullFilter, dialect, inValueLengthLimit).get)
    Assert.assertEquals("(`left` = 5) and (`left` > 5)",
      Utils.compileFilter(validAndFilter, dialect, inValueLengthLimit).get)
    Assert.assertTrue(Utils.compileFilter(invalidAndFilter, dialect, inValueLengthLimit).isEmpty)
    Assert.assertEquals("(`left` = 5) or (`left` > 5)",
      Utils.compileFilter(validOrFilter, dialect, inValueLengthLimit).get)
    Assert.assertTrue(Utils.compileFilter(invalidOrFilter, dialect, inValueLengthLimit).isEmpty)
    Assert.assertEquals("`left` like '%right%'", Utils.compileFilter(stringContainsFilter, dialect, inValueLengthLimit).get)
    Assert.assertEquals("`left` not like '%right%'", Utils.compileFilter(notStringContainsFilter, dialect, inValueLengthLimit).get)
    Assert.assertEquals("`left` like '%right'", Utils.compileFilter(stringEndsWithFilter, dialect, inValueLengthLimit).get)
    Assert.assertEquals("`left` not like '%right'", Utils.compileFilter(notStringEndsWithFilter, dialect, inValueLengthLimit).get)
    Assert.assertEquals("`left` like 'right%'", Utils.compileFilter(stringStartsWithFilter, dialect, inValueLengthLimit).get)
    Assert.assertEquals("`left` not like 'right%'", Utils.compileFilter(notStringStartsWithFilter, dialect, inValueLengthLimit).get)
  }

  @Test
  def testParams(): Unit = {
    val parameters1 = Map(
      DorisOptions.DORIS_TABLE_IDENTIFIER.getName -> "a.b",
      "test_underline" -> "x_y",
      "user" -> "user",
      "password" -> "password"
    )
    val result1 = Utils.params(parameters1, logger)
    Assert.assertEquals("a.b", result1(DorisOptions.DORIS_TABLE_IDENTIFIER.getName))
    Assert.assertEquals("x_y", result1("doris.test.underline"))
    Assert.assertEquals("user", result1("doris.request.auth.user"))
    Assert.assertEquals("password", result1("doris.request.auth.password"))


    val parameters2 = Map(
      DorisOptions.DORIS_TABLE_IDENTIFIER.getName -> "a.b"
    )
    val result2 = Utils.params(parameters2, logger)
    Assert.assertEquals("a.b", result2(DorisOptions.DORIS_TABLE_IDENTIFIER.getName))

    val parameters3 = Map(
      DorisOptions.DORIS_PASSWORD.getName -> "a.b"
    )
    thrown.expect(classOf[DorisException])
    thrown.expectMessage(startsWith(s"${DorisOptions.DORIS_PASSWORD.getName} cannot use in Doris Datasource,"))
    Utils.params(parameters3, logger)

    val parameters4 = Map(
      DorisOptions.DORIS_USER.getName -> "a.b"
    )
    thrown.expect(classOf[DorisException])
    thrown.expectMessage(startsWith(s"${DorisOptions.DORIS_USER.getName} cannot use in Doris Datasource,"))
    Utils.params(parameters4, logger)

    val parameters5 = Map(
      DorisOptions.DORIS_REQUEST_AUTH_PASSWORD -> "a.b"
    )
    thrown.expect(classOf[DorisException])
    thrown.expectMessage(
      startsWith(s"${DorisOptions.DORIS_REQUEST_AUTH_PASSWORD} cannot use in Doris Datasource,"))
    Utils.params(parameters5, logger)

    val parameters6 = Map(
      DorisOptions.DORIS_REQUEST_AUTH_USER -> "a.b"
    )
    thrown.expect(classOf[DorisException])
    thrown.expectMessage(startsWith(s"${DorisOptions.DORIS_REQUEST_AUTH_USER} cannot use in Doris Datasource,"))
    Utils.params(parameters6, logger)
  }

}
