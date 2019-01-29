package org.apache.spark.sql.structured.datasource.custom

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * @author : shirukai
  * @date : 2019-01-25 18:03
  *       自定义数据输入源：需要继承Source接口
  *       实现思路：
  *       （1）通过重写schema方法来指定数据输入源的schema，这个schema需要与Provider中指定的schema保持一致
  *       （2）通过重写getOffset方法来获取数据的偏移量，这个方法会一直被轮询调用，不断的获取偏移量
  *       （3) 通过重写getBatch方法，来获取数据，这个方法是在偏移量发生改变后被触发
  *       （4）通过stop方法，来进行一下关闭资源的操作
  *
  */
class CustomDataSource(sqlContext: SQLContext,
                       parameters: Map[String, String],
                       schemaOption: Option[StructType]) extends Source
  with Logging {

  /**
    * 指定数据源的schema，需要与Provider中sourceSchema中指定的schema保持一直，否则会报异常
    * 触发机制：当创建数据源的时候被触发执行
    *
    * @return schema
    */
  override def schema: StructType = schemaOption.get

  /**
    * 获取offset，用来监控数据的变化情况
    * 触发机制：不断轮询调用
    * 实现要点：
    * （1）Offset的实现：
    * 由函数返回值可以看出，我们需要提供一个标准的返回值Option[Offset]
    * 我们可以通过继承 org.apache.spark.sql.sources.v2.reader.streaming.Offset实现，这里面其实就是保存了个json字符串
    *
    * （2) JSON转化
    * 因为Offset里实现的是一个json字符串，所以我们需要将我们存放offset的集合或者case class转化重json字符串
    * spark里是通过org.json4s.jackson这个包来实现case class 集合类（Map、List、Seq、Set等）与json字符串的相互转化
    *
    * @return Offset
    */
  override def getOffset: Option[Offset] = ???

  /**
    * 获取数据
    *
    * @param start 上一个批次的end offset
    * @param end   通过getOffset获取的新的offset
    *              触发机制：当不断轮询的getOffset方法，获取的offset发生改变时，会触发该方法
    *
    *              实现要点：
    *              （1）DataFrame的创建：
    *              可以通过生成RDD，然后使用RDD创建DataFrame
    *              RDD创建：sqlContext.sparkContext.parallelize(rows.toSeq)
    *              DataFrame创建：sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
    * @return DataFrame
    */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = ???

  /**
    * 关闭资源
    * 将一些需要关闭的资源放到这里来关闭，如MySQL的数据库连接等
    */
  override def stop(): Unit = ???
}
