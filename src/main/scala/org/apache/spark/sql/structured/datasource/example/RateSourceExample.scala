package org.apache.spark.sql.structured.datasource.example

import org.apache.spark.sql.SparkSession

/**
  * @author : shirukai
  * @date : 2019-01-25 20:04
  *       基于RateSource的数据源测试
  */
object RateSourceExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    val rate = spark.readStream
      .format("rate")
      // 每秒生成的行数，默认值为1
      .option("rowsPerSecond", 10)
      .option("numPartitions", 10)
      .load()

    val query =rate.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", value = false)
      .start()

    query.awaitTermination()
  }
}
