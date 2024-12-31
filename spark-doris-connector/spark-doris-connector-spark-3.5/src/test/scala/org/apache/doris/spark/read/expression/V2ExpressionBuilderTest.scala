package org.apache.doris.spark.read.expression

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

import org.apache.spark.sql.sources._
import org.junit.jupiter.api.{Assertions, Test}

class V2ExpressionBuilderTest {

  @Test
  def buildTest(): Unit = {

    val builder = new V2ExpressionBuilder(10)
    Assertions.assertEquals(builder.build(EqualTo("c0", 1).toV2), "`c0` = 1")
    Assertions.assertEquals(builder.build(Not(EqualTo("c1", 2)).toV2), "`c1` != 2")
    Assertions.assertEquals(builder.build(GreaterThan("c2", 3.4).toV2), "`c2` > 3.4")
    Assertions.assertEquals(builder.build(GreaterThanOrEqual("c3", 5.6).toV2), "`c3` >= 5.6")
    Assertions.assertEquals(builder.build(LessThan("c4", 7.8).toV2), "`c4` < 7.8")
    Assertions.assertEquals(builder.build(LessThanOrEqual("c5", BigDecimal(9.1011)).toV2), "`c5` <= 9.1011")
    Assertions.assertEquals(builder.build(StringStartsWith("c6","a").toV2), "`c6` LIKE 'a%'")
    Assertions.assertEquals(builder.build(StringEndsWith("c7", "b").toV2), "`c7` LIKE '%b'")
    Assertions.assertEquals(builder.build(StringContains("c8", "c").toV2), "`c8` LIKE '%c%'")
    Assertions.assertEquals(builder.build(In("c9", Array(12,13,14)).toV2), "`c9` IN (12,13,14)")
    Assertions.assertEquals(builder.build(IsNull("c10").toV2), "`c10` IS NULL")
    Assertions.assertEquals(builder.build(Not(IsNull("c11")).toV2), "`c11` IS NOT NULL")
    Assertions.assertEquals(builder.build(And(EqualTo("c12", 15), EqualTo("c13", 16)).toV2), "(`c12` = 15 AND `c13` = 16)")
    Assertions.assertEquals(builder.build(Or(EqualTo("c14", 17), EqualTo("c15", 18)).toV2), "(`c14` = 17 OR `c15` = 18)")
    Assertions.assertEquals(builder.build(AlwaysTrue.toV2), "1=1")
    Assertions.assertEquals(builder.build(AlwaysFalse.toV2), "1=0")
    Assertions.assertNull(builder.build(In("c19", Array(19,20,21,22,23,24,25,26,27,28,29)).toV2))

  }

}
