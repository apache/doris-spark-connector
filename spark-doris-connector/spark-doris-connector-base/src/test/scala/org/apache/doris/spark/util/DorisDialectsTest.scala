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

package org.apache.doris.spark.util

import org.apache.spark.sql.sources.{And, EqualTo, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Not, Or, StringContains, StringEndsWith, StringStartsWith}
import org.junit.Assert
import org.junit.jupiter.api.Test

import java.sql.{Date, Timestamp}
import java.time.LocalDate

class DorisDialectsTest {

  @Test def compileFilterTest(): Unit = {

    Assert.assertEquals(DorisDialects.compileFilter(EqualTo("c0", 1), 10).get, "`c0` = 1")
    Assert.assertEquals(DorisDialects.compileFilter(Not(EqualTo("c1", 2)), 10).get, "`c1` != 2")
    Assert.assertEquals(DorisDialects.compileFilter(GreaterThan("c3", 3), 10).get, "`c3` > 3")
    Assert.assertEquals(DorisDialects.compileFilter(GreaterThanOrEqual("c4", 4), 10).get, "`c4` >= 4")
    Assert.assertEquals(DorisDialects.compileFilter(LessThan("c5", 5), 10).get, "`c5` < 5")
    Assert.assertEquals(DorisDialects.compileFilter(LessThanOrEqual("c6", 6), 10).get, "`c6` <= 6")
    Assert.assertEquals(DorisDialects.compileFilter(In("c7", Array(7,8,9)), 10).get, "`c7` IN (7,8,9)")
    Assert.assertEquals(DorisDialects.compileFilter(Not(In("c7", Array(10,11,12))), 10).get, "`c7` NOT IN (10,11,12)")
    Assert.assertEquals(DorisDialects.compileFilter(IsNull("c8"), 10).get, "`c8` IS NULL")
    Assert.assertEquals(DorisDialects.compileFilter(IsNotNull("c9"), 10).get, "`c9` IS NOT NULL")
    Assert.assertEquals(DorisDialects.compileFilter(And(EqualTo("c10", 13), EqualTo("c11", 14)), 10).get, "((`c10` = 13) AND (`c11` = 14))")
    Assert.assertEquals(DorisDialects.compileFilter(Or(EqualTo("c12", 15), EqualTo("c13", 16)), 10).get, "((`c12` = 15) OR (`c13` = 16))")
    Assert.assertEquals(DorisDialects.compileFilter(Or(Or(EqualTo("c12", 15), EqualTo("c13", 16)), GreaterThan("c14", 17)), 10).get,
      "((((`c12` = 15) OR (`c13` = 16))) OR (`c14` > 17))")
    Assert.assertEquals(DorisDialects.compileFilter(StringContains("c14", "a"), 10).get, "`c14` LIKE '%a%'")
    Assert.assertEquals(DorisDialects.compileFilter(Not(StringContains("c15", "a")), 10).get, "`c15` NOT LIKE '%a%'")
    Assert.assertEquals(DorisDialects.compileFilter(StringEndsWith("c16", "a"), 10).get, "`c16` LIKE '%a'")
    Assert.assertEquals(DorisDialects.compileFilter(Not(StringEndsWith("c17", "a")), 10).get, "`c17` NOT LIKE '%a'")
    Assert.assertEquals(DorisDialects.compileFilter(StringStartsWith("c18", "a"), 10).get, "`c18` LIKE 'a%'")
    Assert.assertEquals(DorisDialects.compileFilter(Not(StringStartsWith("c19", "a")), 10).get, "`c19` NOT LIKE 'a%'")

  }

  @Test def compileValueTest(): Unit = {

    Assert.assertEquals(DorisDialects.compileValue("test"), "'test'")
    Assert.assertEquals(DorisDialects.compileValue(Timestamp.valueOf("2024-01-01 00:00:00")), "'2024-01-01 00:00:00.0'")
    Assert.assertEquals(DorisDialects.compileValue(Date.valueOf("2024-01-01")), "'2024-01-01'")
    Assert.assertEquals(DorisDialects.compileValue(LocalDate.of(2024,1,1)), "'2024-01-01'")
    Assert.assertEquals(DorisDialects.compileValue(Array[Int](1,2,3)), "[1,2,3]")
    Assert.assertEquals(DorisDialects.compileValue(4), 4)

  }

  @Test def quoteTest(): Unit = {
    Assert.assertEquals(DorisDialects.quote("test"), "`test`")
  }

  @Test def escapeSqlTest(): Unit = {
    Assert.assertNull(DorisDialects.escapeSql(null))
    Assert.assertEquals(DorisDialects.escapeSql("'test'"), "''test''")
  }

}
