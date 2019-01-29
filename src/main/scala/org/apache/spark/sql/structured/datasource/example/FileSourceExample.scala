package org.apache.spark.sql.structured.datasource.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
  * @author : shirukai
  * @date : 2019-01-25 19:18
  *       文件数据源测试
  */
object FileSourceExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    val source = spark
      .readStream
      // Schema must be specified when creating a streaming source DataFrame.
      .schema(StructType(List(
      StructField("name", StringType),
      StructField("value", IntegerType)
    )))
      // 每个trigger最大文件数量
      .option("maxFilesPerTrigger", 100)
      // 是否首先计算最新的文件，默认为false
      .option("latestFirst", value = true)
      // 是否值检查名字，如果名字相同，则不视为更新，默认为false
      .option("fileNameOnly", value = true)
      .csv("*.csv")

    val query = source.writeStream
      .outputMode("update")
      .format("console")
      //.option("checkpointLocation", checkpointLocation)
      .option("truncate", value = false)
      .start()

    query.awaitTermination()
  }
}
