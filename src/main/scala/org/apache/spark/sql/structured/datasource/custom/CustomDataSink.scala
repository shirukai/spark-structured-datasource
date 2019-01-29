package org.apache.spark.sql.structured.datasource.custom

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * @author : shirukai
  * @date : 2019-01-25 18:03
  *       自定义数据输出源
  */
class CustomDataSink(sqlContext: SQLContext,
                     parameters: Map[String, String],
                     outputMode: OutputMode) extends Sink with Logging {

  /**
    * 添加Batch，即数据写出
    *
    * @param batchId batchId
    * @param data    DataFrame
    *                触发机制：当发生计算时，会触发该方法，并且得到要输出的DataFrame
    *                实现摘要：
    *                1. 数据写入方式：
    *                （1）通过SparkSQL内置的数据源写出
    *                我们拿到DataFrame之后可以通过SparkSQL内置的数据源将数据写出，如：
    *                JSON数据源、CSV数据源、Text数据源、Parquet数据源、JDBC数据源等。
    *                （2）通过自定义SparkSQL的数据源进行写出
    *                （3）通过foreachPartition 将数据写出
    */
  override def addBatch(batchId: Long, data: DataFrame): Unit = ???
}
