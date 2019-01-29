package org.apache.spark.sql.structured.datasource.example

import java.util.UUID

import org.apache.spark.sql.SparkSession

/**
  * @author : shirukai
  * @date : 2019-01-26 09:58
  *       Spark Structured 内置ConsoleSink用例
  */
object ConsoleSinkExample {
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

    val consoleSink = source.writeStream
      .format("console")
      // 是否压缩显示
      .option("truncate", value = false)
      // 显示条数
      .option("numRows", 30)
      .option("checkpointLocation", "/tmp/temporary-" + UUID.randomUUID.toString)
      .start()

    consoleSink.awaitTermination()
  }
}
