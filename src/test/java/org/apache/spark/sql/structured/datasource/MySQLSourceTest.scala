package org.apache.spark.sql.structured.datasource

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

/**
  * @author : shirukai
  * @date : 2019-01-29 09:57
  *       测试自定义MySQLSource
  */
object MySQLSourceTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()
    val schema = StructType(List(
      StructField("name", StringType),
      StructField("creatTime", TimestampType),
      StructField("modifyTime", TimestampType)
    )
    )
    val options = Map[String, String](
      "driverClass" -> "com.mysql.cj.jdbc.Driver",
      "jdbcUrl" -> "jdbc:mysql://localhost:3306/spark-source?useSSL=false&characterEncoding=utf-8",
      "user" -> "root",
      "password" -> "hollysys",
      "tableName" -> "model")
    val source = spark
      .readStream
      .format("org.apache.spark.sql.structured.datasource.MySQLSourceProvider")
      .options(options)
      .schema(schema)
      .load()

    import org.apache.spark.sql.functions._
    val query = source.groupBy("creatTime").agg(collect_list("name").cast(StringType).as("names")).writeStream
      .outputMode("update")
      .format("org.apache.spark.sql.structured.datasource.MySQLSourceProvider")
      .option("checkpointLocation", "/tmp/MySQLSourceProvider11")
      .option("user","root")
      .option("password","hollysys")
      .option("dbtable","test")
      .option("url","jdbc:mysql://localhost:3306/spark-source?useSSL=false&characterEncoding=utf-8")
      .start()

    query.awaitTermination()
  }
}
