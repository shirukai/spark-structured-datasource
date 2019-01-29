package org.apache.spark.sql.structured.datasource.example

import java.util.UUID

import org.apache.spark.sql.SparkSession

/**
  * @author : shirukai
  * @date : 2019-01-26 09:58
  *       Spark Structured 内置MemorySink用例
  */
object MemorySinkExample {
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

    val memorySink = source.writeStream
      .format("memory")
      .queryName("memorySinkTable")
      .option("checkpointLocation", "/tmp/temporary-" + UUID.randomUUID.toString)
      .start()


    new Thread(new Runnable {
      override def run(): Unit = {
        while (true) {
          spark.sql("select * from memorySinkTable").show(false)
          Thread.sleep(1000)
        }
      }
    }).start()
    memorySink.awaitTermination()
  }
}
