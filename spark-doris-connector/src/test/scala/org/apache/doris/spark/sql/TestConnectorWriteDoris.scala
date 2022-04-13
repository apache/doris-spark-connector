package org.apache.doris.spark.sql

import org.apache.spark.sql.SparkSession
import org.junit.Test

class TestConnectorWriteDoris {

  val dorisFeNodes = "127.0.0.1:8030"
  val dorisUser = "root"
  val dorisPwd = ""
  val dorisTable = "test.test_order"

  val kafkaServers = "127.0.0.1:9093"
  val kafkaTopics = "test_spark"

  @Test
  def listDataWriteTest(): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val df = spark.createDataFrame(Seq(
      ("1", 100, "待付款"),
      ("2", 200, "待发货"),
      ("3", 300, "已收货")
    )).toDF("order_id", "order_amount", "order_status")
    df.write
      .format("doris")
      .option("doris.fenodes", dorisFeNodes)
      .option("doris.table.identifier", dorisTable)
      .option("user", dorisUser)
      .option("password", dorisPwd)
      .option("sink.batch.size", 2)
      .option("sink.max-retries", 2)
      .save()
    spark.stop()
  }


  @Test
  def csvDataWriteTest(): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val df = spark.read
      .option("header", "true") // uses the first line as names of columns
      .option("inferSchema", "true") // infers the input schema automatically from data
      .csv("data.csv")
    df.createTempView("tmp_tb")
    val doris = spark.sql(
      """
        |create  TEMPORARY VIEW test_lh
        |USING doris
        |OPTIONS(
        | "table.identifier"="test.test_lh",
        | "fenodes"="127.0.0.1:8030",
        | "user"="root",
        | "password"=""
        |);
        |""".stripMargin)
    spark.sql(
      """
        |insert into test_lh select  name,gender,age from tmp_tb ;
        |""".stripMargin)
    spark.stop()
  }

  @Test
  def structuredStreamingWriteTest(): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .getOrCreate()
    val df = spark.readStream
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("startingOffsets", "latest")
      .option("subscribe", kafkaTopics)
      .format("kafka")
      .option("failOnDataLoss", false)
      .load()

    df.selectExpr("CAST(value AS STRING)")
      .writeStream
      .format("doris")
      .option("checkpointLocation", "/tmp/test")
      .option("doris.table.identifier", dorisTable)
      .option("doris.fenodes", dorisFeNodes)
      .option("user", dorisUser)
      .option("password", dorisPwd)
      .option("sink.batch.size", 2)
      .option("sink.max-retries", 2)
      .start().awaitTermination()
  }

}
