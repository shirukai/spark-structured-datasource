package org.apache.spark.sql.structured.datasource.example

import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType

/**
  * @author : shirukai
  * @date : 2019-01-26 09:58
  *       Spark Structured 内置KafkaSink用例
  */
object KafkaSinkExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    val source = spark.readStream
      .format("rate")
      // 每秒生成的行数，默认值为1
      .option("rowsPerSecond", 10)
      .option("numPartitions", 10)
      .load()
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val kafkaSink = source.select(array(to_json(struct("*"))).as("value").cast(StringType),
      $"timestamp".as("key").cast(StringType)).writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("checkpointLocation", "/tmp/temporary-" + UUID.randomUUID.toString)
      .option("topic", "hiacloud-ts-dev")
      .start()


    kafkaSink.awaitTermination()
  }
}
