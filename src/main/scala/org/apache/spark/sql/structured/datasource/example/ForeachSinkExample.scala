package org.apache.spark.sql.structured.datasource.example

import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

/**
  * @author : shirukai
  * @date : 2019-01-26 09:58
  *       Spark Structured 内置ForeachSink用例
  */
object ForeachSinkExample {
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

    val foreachSink = source.writeStream
        .foreach(new ForeachWriter[Row] {
          override def open(partitionId: Long, version: Long): Boolean = {
            println(s"partitionId=$partitionId,version=$version")
            true

          }

          override def process(value: Row): Unit = {
            println(value)
          }

          override def close(errorOrNull: Throwable): Unit = {
            println("close")
          }
        })
      .start()

    foreachSink.awaitTermination()
  }
}
