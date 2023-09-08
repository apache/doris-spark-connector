package org.apache.doris.spark.sql

import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Ignore, Test}

import java.sql.{Date, Timestamp}
import scala.collection.JavaConverters._

@Ignore
class SchemaUtilsTest {

  @Test
  def rowColumnValueTest(): Unit = {

    val spark = SparkSession.builder().master("local").getOrCreate()

    val df = spark.createDataFrame(Seq(
      (1, Date.valueOf("2023-09-08"), Timestamp.valueOf("2023-09-08 17:00:00"), Array(1, 2, 3), Map[String, String]("a" -> "1"))
    )).toDF("c1", "c2", "c3", "c4", "c5")

    val schema = df.schema

    df.queryExecution.toRdd.foreach(row => {

      val fields = schema.fields
      Assert.assertEquals(1, SchemaUtils.rowColumnValue(row, 0, fields(0).dataType))
      Assert.assertEquals("2023-09-08", SchemaUtils.rowColumnValue(row, 1, fields(1).dataType))
      Assert.assertEquals("2023-09-08 17:00:00.0", SchemaUtils.rowColumnValue(row, 2, fields(2).dataType))
      Assert.assertEquals("[1,2,3]", SchemaUtils.rowColumnValue(row, 3, fields(3).dataType))
      println(SchemaUtils.rowColumnValue(row, 4, fields(4).dataType))
      Assert.assertEquals(Map("a" -> "1").asJava, SchemaUtils.rowColumnValue(row, 4, fields(4).dataType))

    })

  }

}
