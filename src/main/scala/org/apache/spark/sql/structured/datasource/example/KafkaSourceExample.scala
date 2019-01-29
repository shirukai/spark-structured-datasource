package org.apache.spark.sql.structured.datasource.example

import org.apache.spark.sql.SparkSession

/**
  * @author : shirukai
  * @date : 2019-01-26 09:46
  *       Kafka输入源测试
  */
object KafkaSourceExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    val source = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "hiacloud-ts-dev")
      //.option("startingOffsets", "earliest")
      .option("failOnDataLoss", "true")
      .load()

    val query = source.writeStream
      .outputMode("update")
      .format("console")
      //.option("checkpointLocation", checkpointLocation)
      .option("truncate", value = false)
      .start()

    query.awaitTermination()

  }
}
