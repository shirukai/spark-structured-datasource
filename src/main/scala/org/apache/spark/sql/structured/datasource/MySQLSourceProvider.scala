package org.apache.spark.sql.structured.datasource

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider, StreamSourceProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

/**
  * @author : shirukai
  * @date : 2019-01-25 09:10
  *       自定义MySQL数据源
  */
class MySQLSourceProvider extends DataSourceRegister
  with StreamSourceProvider
  with StreamSinkProvider
  with Logging {
  /**
    * 数据源的描述名字，如：kafka、socket
    *
    * @return 字符串shotName
    */
  override def shortName(): String = "mysql"


  /**
    * 定义数据源的Schema
    *
    * @param sqlContext   Spark SQL 上下文
    * @param schema       通过.schema()方法传入的schema
    * @param providerName Provider的名称，包名+类名
    * @param parameters   通过.option()方法传入的参数
    * @return 元组，(shotName,schema)
    */
  override def sourceSchema(
                             sqlContext: SQLContext,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): (String, StructType) = {
    (providerName, schema.get)
  }

  /**
    * 创建输入源
    *
    * @param sqlContext   Spark SQL 上下文
    * @param metadataPath 元数据Path
    * @param schema       通过.schema()方法传入的schema
    * @param providerName Provider的名称，包名+类名
    * @param parameters   通过.option()方法传入的参数
    * @return 自定义source，需要继承Source接口实现
    */
  override def createSource(
                             sqlContext: SQLContext,
                             metadataPath: String, schema: Option[StructType],
                             providerName: String, parameters: Map[String, String]): Source = new MySQLSource(sqlContext, parameters, schema)

  /**
    * 创建输出源
    *
    * @param sqlContext       Spark SQL 上下文
    * @param parameters       通过.option()方法传入的参数
    * @param partitionColumns 分区列名?
    * @param outputMode       输出模式
    * @return
    */
  override def createSink(
                           sqlContext: SQLContext,
                           parameters: Map[String, String],
                           partitionColumns: Seq[String], outputMode: OutputMode): Sink = new MySQLSink(sqlContext: SQLContext,parameters, outputMode)
}
