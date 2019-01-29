# StructuredStreaming 内置数据源及实现自定义数据源

> 版本说明：
>
> Spark:2.3/2.4
>
> 代码仓库：https://github.com/shirukai/spark-structured-datasource.git

# 1 Structured内置的输入源 Source

官网文档：http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#input-sources

| Source        | Options                                                      | Fault-tolerant | Notes                                      |
| ------------- | ------------------------------------------------------------ | -------------- | ------------------------------------------ |
| File Source   | maxFilesPerTrigger：每个触发器中要考虑的最大新文件数（默认值：无最大值）latestFirst：是否先处理最新的新文件，当存在大量积压的文件时有用（默认值：false） <br/>fileNameOnly：是否基于以下方法检查新文件只有文件名而不是完整路径（默认值：false）。 | 支持容错       | 支持glob路径，但不支持以口号分割的多个路径 |
| Socket Source | host：要连接的主机，必须指定<br/>port：要连接的端口，必须指定 | 不支持容错     |                                            |
| Rate Source   | rowsPerSecond（例如100，默认值：1）：每秒应生成多少行。<br/>rampUpTime（例如5s，默认值：0s）：在生成速度变为之前加速多长时间rowsPerSecond。使用比秒更精细的粒度将被截断为整数秒。numPartitions（例如10，默认值：Spark的默认并行性）：生成的行的分区号 | 支持容错       |                                            |
| Kafka Source  | http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html | 支持容错       |                                            |
## 1.1 File Source

将目录中写入的文件作为数据流读取。支持的文件格式为：text、csv、json、orc、parquet

**用例**

代码位置：org.apache.spark.sql.structured.datasource.example

```scala
val source = spark
  .readStream
  // Schema must be specified when creating a streaming source DataFrame.
  .schema(schema)
  // 每个trigger最大文件数量
  .option("maxFilesPerTrigger",100)
  // 是否首先计算最新的文件，默认为false
  .option("latestFirst",value = true)
  // 是否值检查名字，如果名字相同，则不视为更新，默认为false
  .option("fileNameOnly",value = true)
  .csv("*.csv")
```

## 1.2 Socket Source

从Socket中读取UTF8文本数据。一般用于测试，使用nc -lc 端口号 向Socket监听的端口发送数据。

**用例**

代码位置：org.apache.spark.sql.structured.datasource.example

```scala
val lines = spark.readStream
  .format("socket")
  .option("host", "localhost")
  .option("port", 9090)
  .load()
```

## 1.3 Rate Source

以每秒指定的行数生成数据，每个输出行包含一个`timestamp`和`value`。其中`timestamp`是一个`Timestamp`含有信息分配的时间类型，并且`value`是`Long`包含消息的计数从0开始作为第一行类型。此源用于测试和基准测试。

**用例**

代码位置：org.apache.spark.sql.structured.datasource.example

```scala
    val rate = spark.readStream
      .format("rate")
      // 每秒生成的行数，默认值为1
      .option("rowsPerSecond", 10)
      .option("numPartitions", 10)
      .option("rampUpTime",0)
      .option("rampUpTime",5)
      .load()
```

## 1.4 Kafka Source

官网文档：http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html

**用例**

代码位置：org.apache.spark.sql.structured.datasource.example

```scala
val df = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribePattern", "topic.*")
  .load()
```

# 2 Structured 内置的输出源 Sink

| Sink              | Supported Output Modes | Options                                                      | Fault-tolerant            | Notes                                                        |
| ----------------- | ---------------------- | ------------------------------------------------------------ | ------------------------- | ------------------------------------------------------------ |
| File Sink         | Append                 | path：输出路径（必须指定）                                   | 支持容错（exactly-once）  | 支持分区写入                                                 |
| Kafka Sink        | Append,Update,Complete | See the [Kafka Integration Guide](http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html) | 支持容错（at-least-once） | [Kafka Integration Guide](http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html) |
| Foreach Sink      | Append,Update,Complete | None                                                         |                           | [Foreach Guide](http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#using-foreach-and-foreachbatch) |
| ForeachBatch Sink | Append,Update,Complete | None                                                         |                           | [Foreach Guide](http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#using-foreach-and-foreachbatch) |
| Console Sink      | Append,Update,Complete | numRows：每次触发器打印的行数（默认值：20） <br/>truncate：是否过长时截断输出（默认值：true |                           |                                                              |
| Memory Sink       | Append,Complete        | None                                                         |                           | 表名是查询的名字                                             |

## 2.1 File Sink

将结果输出到文件，支持格式parquet、csv、orc、json等

**用例**

代码位置：org.apache.spark.sql.structured.datasource.example

```scala
val fileSink = source.writeStream
  .format("parquet")
  //.format("csv")
  //.format("orc")
 // .format("json")
  .option("path", "data/sink")
  .option("checkpointLocation", "/tmp/temporary-" + UUID.randomUUID.toString)
  .start()
```

## 2.2 Console Sink

将结果输出到控制台

**用例**

代码位置：org.apache.spark.sql.structured.datasource.example

```scala
    val consoleSink = source.writeStream
      .format("console")
      // 是否压缩显示
      .option("truncate", value = false)
      // 显示条数
      .option("numRows", 30)
      .option("checkpointLocation", "/tmp/temporary-" + UUID.randomUUID.toString)
      .start()

```

## 2.3 Memory Sink

将结果输出到内存，需要指定内存中的表名。可以使用sql进行查询

**用例**

代码位置：org.apache.spark.sql.structured.datasource.example

````scala

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
````

## 2.4 Kafka Sink

将结果输出到Kafka，需要将DataFrame转成key，value两列，或者topic、key、value三列

**用例**

代码位置：org.apache.spark.sql.structured.datasource.example

```scala
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val kafkaSink = source.select(array(to_json(struct("*"))).as("value").cast(StringType),
      $"timestamp".as("key").cast(StringType)).writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("checkpointLocation", "/tmp/temporary-" + UUID.randomUUID.toString)
      .option("topic", "hiacloud-ts-dev")
      .start()
```

## 2.5 ForeachBatch Sink(2.4)

适用于对于一个批次来说应用相同的写入方式的场景。方法传入这个batch的DataFrame以及batchId。这个方法在2.3之后的版本才有而且仅支持微批模式。

![](http://shirukai.gitee.io/images/4a5973ac848b33f8b5938be7e7754b85.jpg)

**用例**

代码位置：org.apache.spark.sql.structured.datasource.example

```scala
    val foreachBatchSink = source.writeStream.foreachBatch((batchData: DataFrame, batchId) => {
      batchData.show(false)
    }).start()
```

## 2.6 Foreach Sink

Foreach 每一条记录，通过继承ForeachWriter[Row]，实现open()，process()，close()方法。在open方法了我们可以获取一个资源连接，如MySQL的连接。在process里我们可以获取一条记录，并处理这条数据发送到刚才获取资源连接的MySQL中，在close里我们可以关闭资源连接。注意，foreach是对Partition来说的，同一个分区只会调用一次open、close方法，但对于每条记录来说，都会调用process方法。

**用例**

代码位置：org.apache.spark.sql.structured.datasource.example

```scala
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

```

# 3 自定义输入源

某些应用场景下我们可能需要自定义数据源，如业务中，需要在获取KafkaSource的同时，动态从缓存中或者http请求中加载业务数据，或者是其它的数据源等都可以参考规范自定义。自定义输入源需要以下步骤：

第一步：继承DataSourceRegister和StreamSourceProvider创建自定义Provider类

第二步：重写DataSourceRegister类中的shotName和StreamSourceProvider中的createSource以及sourceSchema方法

第三步：继承Source创建自定义Source类

第四步：重写Source中的schema方法指定输入源的schema

第五步：重写Source中的getOffest方法监听流数据

第六步：重写Source中的getBatch方法获取数据

 第七步：重写Source中的stop方法用来关闭资源

## 3.1 创建CustomDataSourceProvider类

### 3.1.1 继承DataSourceRegister和StreamSourceProvider

要创建自定义的DataSourceProvider必须要继承位于org.apache.spark.sql.sources包下的DataSourceRegister以及该包下的StreamSourceProvider。如下所示：

```scala
class CustomDataSourceProvider extends DataSourceRegister
  with StreamSourceProvider
  with Logging {
      //Override some functions ……
  }
```

### 3.1.2 重写DataSourceRegister的shotName方法

该方法用来指定一个数据源的名字，用来想spark注册该数据源。如Spark内置的数据源的shotName:kafka

、socket、rate等，该方法返回一个字符串，如下所示：

```scala
  /**
    * 数据源的描述名字，如：kafka、socket
    *
    * @return 字符串shotName
    */
  override def shortName(): String = "custom"
```

### 3.1.3 重写StreamSourceProvider中的sourceSchema方法

该方法是用来定义数据源的schema，可以使用用户传入的schema，也可以根据传入的参数进行动态创建。返回值是个二元组(shotName,scheam)，代码如下所示：

```scala
  /**
    * 定义数据源的Schema
    *
    * @param sqlContext   Spark SQL 上下文
    * @param schema       通过.schema()方法传入的schema
    * @param providerName Provider的名称，包名+类名
    * @param parameters   通过.option()方法传入的参数
    * @return 元组，(shotName,schema)
    */
  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = (shortName(),schema.get)
```

### 3.1.4 重写StreamSourceProvider中的createSource方法

通过传入的参数，来实例化我们自定义的DataSource，是我们自定义Source的重要入口的地方

```scala
/**
  * 创建输入源
  *
  * @param sqlContext   Spark SQL 上下文
  * @param metadataPath 元数据Path
  * @param schema       通过.schema()方法传入的schema
  * @param providerName Provider的名称，包名+类名
  * @param parameters   通过.option()方法传入的参数
  * @return 自定义source，需要继承Source接口实现
  **/

override def createSource(sqlContext: SQLContext,
                          metadataPath: String,
                          schema: Option[StructType],
                          providerName: String,
                          parameters: Map[String, String]): Source = new CustomDataSource(sqlContext,parameters,schema)
```

### 3.1.5 CustomDataSourceProvider.scala完整代码

```scala
package org.apache.spark.sql.structured.datasource.custom

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider, StreamSourceProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

/**
  * @author : shirukai
  * @date : 2019-01-25 17:49
  *       自定义Structured Streaming数据源
  *
  *       （1）继承DataSourceRegister类
  *       需要重写shortName方法，用来向Spark注册该组件
  *
  *       （2）继承StreamSourceProvider类
  *       需要重写createSource以及sourceSchema方法，用来创建数据输入源
  *
  *       （3）继承StreamSinkProvider类
  *       需要重写createSink方法，用来创建数据输出源
  *
  *
  */
class CustomDataSourceProvider extends DataSourceRegister
  with StreamSourceProvider
  with StreamSinkProvider
  with Logging {


  /**
    * 数据源的描述名字，如：kafka、socket
    *
    * @return 字符串shotName
    */
  override def shortName(): String = "custom"


  /**
    * 定义数据源的Schema
    *
    * @param sqlContext   Spark SQL 上下文
    * @param schema       通过.schema()方法传入的schema
    * @param providerName Provider的名称，包名+类名
    * @param parameters   通过.option()方法传入的参数
    * @return 元组，(shotName,schema)
    */
  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = (shortName(),schema.get)

  /**
    * 创建输入源
    *
    * @param sqlContext   Spark SQL 上下文
    * @param metadataPath 元数据Path
    * @param schema       通过.schema()方法传入的schema
    * @param providerName Provider的名称，包名+类名
    * @param parameters   通过.option()方法传入的参数
    * @return 自定义source，需要继承Source接口实现
    **/

  override def createSource(sqlContext: SQLContext,
                            metadataPath: String,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): Source = new CustomDataSource(sqlContext,parameters,schema)


  /**
    * 创建输出源
    *
    * @param sqlContext       Spark SQL 上下文
    * @param parameters       通过.option()方法传入的参数
    * @param partitionColumns 分区列名?
    * @param outputMode       输出模式
    * @return
    */
  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = new CustomDataSink(sqlContext,parameters,outputMode)
}

```

## 3.2 创建CustomDataSource类

### 3.2.1 继承Source创建CustomDataSource类

要创建自定义的DataSource必须要继承位于org.apache.spark.sql.sources包下的Source。如下所示：

```scala
class CustomDataSource(sqlContext: SQLContext,
                       parameters: Map[String, String],
                       schemaOption: Option[StructType]) extends Source
  with Logging {
  //Override some functions ……
}
```

### 3.2.2 重写Source的schema方法

指定数据源的schema，需要与Provider中的sourceSchema指定的schema保持一致，否则会报异常

```scala
  /**
    * 指定数据源的schema，需要与Provider中sourceSchema中指定的schema保持一直，否则会报异常
    * 触发机制：当创建数据源的时候被触发执行
    *
    * @return schema
    */
  override def schema: StructType = schemaOption.get
```

### 3.2.3 重写Source的getOffset方法

此方法是Spark不断的轮询执行的，目的是用来监控流数据的变化情况，一旦数据发生变化，就会触发getBatch方法用来获取数据。

```scala
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
```

### 3.2.4 重写Source的getBatch方法

此方法是Spark用来获取数据的，getOffset方法检测的数据发生变化的时候，会触发该方法， 传入上一次触发时的end Offset作为当前batch的start Offset，将新的offset作为end Offset。

```scala
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
```

### 3.2.5 重写Source的stop方法

用来关闭一些需要关闭或停止的资源及进程

```scala
  /**
    * 关闭资源
    * 将一些需要关闭的资源放到这里来关闭，如MySQL的数据库连接等
    */
  override def stop(): Unit = ???
```

### 3.2.6 CustomDataSource.scala完整代码

```scala
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
```

## 3.3 自定义DataSource的使用

自定义DataSource的使用与内置DataSource一样，只需要在format里指定一下我们的Provider类路径即可。如

```scala
    val source = spark
      .readStream
      .format("org.apache.spark.sql.kafka010.CustomSourceProvider")
      .options(options)
      .schema(schema)
      .load()
```

## 3.4 实现MySQL自定义数据源

此例子仅仅是为了演示如何自定义数据源，与实际业务场景无关。

### 3.4.1 创建MySQLSourceProvider.scala

```scala
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
```

### 3.4.2 创建MySQLSource.scala

```scala
package org.apache.spark.sql.structured.datasource

import java.sql.Connection

import org.apache.spark.executor.InputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}


/**
  * @author : shirukai
  * @date : 2019-01-25 09:41
  */
class MySQLSource(sqlContext: SQLContext,
                  options: Map[String, String],
                  schemaOption: Option[StructType]) extends Source with Logging {

  lazy val conn: Connection = C3p0Utils.getDataSource(options).getConnection

  val tableName: String = options("tableName")

  var currentOffset: Map[String, Long] = Map[String, Long](tableName -> 0)

  val maxOffsetPerBatch: Option[Long] = Option(100)

  val inputMetrics = new InputMetrics()

  override def schema: StructType = schemaOption.get

  /**
    * 获取Offset
    * 这里监控MySQL数据库表中条数变化情况
    * @return Option[Offset]
    */
  override def getOffset: Option[Offset] = {
    val latest = getLatestOffset
    val offsets = maxOffsetPerBatch match {
      case None => MySQLSourceOffset(latest)
      case Some(limit) =>
        MySQLSourceOffset(rateLimit(limit, currentOffset, latest))
    }
    Option(offsets)
  }

  /**
    * 获取数据
    * @param start 上一次的offset
    * @param end 最新的offset
    * @return df
    */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {

    var offset: Long = 0
    if (start.isDefined) {
      offset = offset2Map(start.get)(tableName)
    }
    val limit = offset2Map(end)(tableName) - offset
    val sql = s"SELECT * FROM $tableName limit $limit offset $offset"

    val st = conn.prepareStatement(sql)
    val rs = st.executeQuery()
    val rows: Iterator[InternalRow] = JdbcUtils.resultSetToSparkInternalRows(rs, schemaOption.get, inputMetrics) //todo 好用
    val rdd = sqlContext.sparkContext.parallelize(rows.toSeq)

    currentOffset = offset2Map(end)

    sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
  }

  override def stop(): Unit = {
    conn.close()
  }

  def rateLimit(limit: Long, currentOffset: Map[String, Long], latestOffset: Map[String, Long]): Map[String, Long] = {
    val co = currentOffset(tableName)
    val lo = latestOffset(tableName)
    if (co + limit > lo) {
      Map[String, Long](tableName -> lo)
    } else {
      Map[String, Long](tableName -> (co + limit))
    }
  }

  // 获取最新条数
  def getLatestOffset: Map[String, Long] = {
    var offset: Long = 0
    val sql = s"SELECT COUNT(1) FROM $tableName"
    val st = conn.prepareStatement(sql)
    val rs = st.executeQuery()
    while (rs.next()) {
      offset = rs.getLong(1)
    }
    Map[String, Long](tableName -> offset)
  }

  def offset2Map(offset: Offset): Map[String, Long] = {
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    Serialization.read[Map[String, Long]](offset.json())
  }
}

case class MySQLSourceOffset(offset: Map[String, Long]) extends Offset {
  implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

  override def json(): String = Serialization.write(offset)
}
```

### 3.4.3 测试MySQLSource

```scala
package org.apache.spark.sql.structured.datasource

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

/**
  * @author : shirukai
  * @date : 2019-01-25 15:12
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
    val query = source.writeStream.format("console")
      // 是否压缩显示
      .option("truncate", value = false)
      // 显示条数
      .option("numRows", 30)
      .option("checkpointLocation", "/tmp/temporary-" + UUID.randomUUID.toString)
      .start()
    query.awaitTermination()
  }
}

```

# 4 自定义输出源

相比较输入源的自定义性，输出源自定义的应用场景貌似更为常用。比如：数据写入关系型数据库、数据写入HBase、数据写入Redis等等。其实Structured提供的foreach以及2.4版本的foreachBatch方法已经可以实现绝大数的应用场景的，几乎是数据想写到什么地方都能实现。但是想要更优雅的实现，我们可以参考Spark SQL Sink规范，通过自定义的Sink的方式来实现。实现自定义Sink需要以下四个个步骤：

第一步：继承DataSourceRegister和StreamSinkProvider创建自定义SinkProvider类

第二步：重写DataSourceRegister类中的shotName和StreamSinkProvider中的createSink方法

第三步：继承Sink创建自定义Sink类

第四步：重写Sink中的addBatch方法

## 4.1 改写CustomDataSourceProvider类

### 4.1.1 新增继承StreamSinkProvider

在上面创建自定义输入源的基础上，新增继承StreamSourceProvider。如下所示：

```scala
class CustomDataSourceProvider extends DataSourceRegister
  with StreamSourceProvider
  with StreamSinkProvider
  with Logging {
      //Override some functions ……
  }
```

### 4.1.2 重写StreamSinkProvider中的createSink方法

通过传入的参数，来实例化我们自定义的DataSink，是我们自定义Sink的重要入口的地方

```scala
  /**
    * 创建输出源
    *
    * @param sqlContext       Spark SQL 上下文
    * @param parameters       通过.option()方法传入的参数
    * @param partitionColumns 分区列名?
    * @param outputMode       输出模式
    * @return
    */
  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = new CustomDataSink(sqlContext,parameters,outputMode)
```

## 4.2 创建CustomDataSink类

### 4.2.1 继承Sink创建CustomDataSink类

要创建自定义的DataSink必须要继承位于org.apache.spark.sql.sources包下的Sink。如下所示：

```scala
class CustomDataSink(sqlContext: SQLContext,
                     parameters: Map[String, String],
                     outputMode: OutputMode) extends Sink with Logging {
    // Override some functions
}
```

### 4.2.2 重写Sink中的addBatch方法

该方法是当发生计算时会被触发，传入的是一个batchId和dataFrame，拿到DataFrame之后，我们有三种写出方式，第一种是使用Spark SQL内置的Sink写出，如 JSON数据源、CSV数据源、Text数据源、Parquet数据源、JDBC数据源等。第二种是通过DataFrame的foreachPartition写出。第三种就是自定义SparkSQL的输出源然后写出。

```scala
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
```

**注意**：

当我们使用第一种方式的时候要注意，此时拿到的DataFrame是一个流式的DataFrame，即isStreaming=ture，通过查看KafkaSink，如下代码所示，先是通过DataFrame.queryExecution执行查询,然后在wite里转成rdd，通过rdd的foreachPartition实现。同样的思路，我们可以利用这个rdd和schema,利用sqlContext.internalCreateDataFrame(rdd, data.schema)重新生成DataFrame，这个在MySQLSink中使用过。

```scala
override def addBatch(batchId: Long, data: DataFrame): Unit = {
  if (batchId <= latestBatchId) {
    logInfo(s"Skipping already committed batch $batchId")
  } else {
    KafkaWriter.write(sqlContext.sparkSession,
      data.queryExecution, executorKafkaParams, topic)
    latestBatchId = batchId
  }
}

  def write(
      sparkSession: SparkSession,
      queryExecution: QueryExecution,
      kafkaParameters: ju.Map[String, Object],
      topic: Option[String] = None): Unit = {
    val schema = queryExecution.analyzed.output
    validateQuery(schema, kafkaParameters, topic)
    queryExecution.toRdd.foreachPartition { iter =>
      val writeTask = new KafkaWriteTask(kafkaParameters, schema, topic)
      Utils.tryWithSafeFinally(block = writeTask.execute(iter))(
        finallyBlock = writeTask.close())
    }
  }
```



## 4.3 自定义DataSink的使用

自定义DataSink的使用与自定义DataSource的使用相同，在format里指定一些类Provider的类路径即可。

```scala
val query = source.groupBy("creatTime").agg(collect_list("name")).writeStream
      .outputMode("update")
      .format("org.apache.spark.sql.kafka010.CustomDataSourceProvider")
      .option(options)
      .start()
    query.awaitTermination()
```

## 4.4 实现MySQL自定义输出源

### 4.4.1 修改MySQLSourceProvider.scala

上面我们实现MySQL自定义输入源的时候，已经创建了MySQLSourceProvider类，我们需要在这个基础上新增继承StreamSinkProvider，并重写createSink方法，如下所示：

```scala
package org.apache.spark.sql.structured.datasource

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.kafka010.{MySQLSink, MySQLSource}
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
      
  //……省略自定义输入源的方法

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

```

### 4.4.1 创建MySQLSink.scala

```scala
package org.apache.spark.sql.structured.datasource

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode

/**
  * @author : shirukai
  * @date : 2019-01-25 17:35
  */
class MySQLSink(sqlContext: SQLContext,parameters: Map[String, String], outputMode: OutputMode) extends Sink with Logging {
  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val query = data.queryExecution
    val rdd = query.toRdd
    val df = sqlContext.internalCreateDataFrame(rdd, data.schema)
    df.show(false)
    df.write.format("jdbc").options(parameters).mode(SaveMode.Append).save()
  }
}
```

### 4.2.3 测试MySQLSink

```scala
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
```

# 3 总结

通过上面的笔记，参看官网文档，可以学习到Structured支持的几种输入源：File Source、Socket Source、Rate Source、Kafka Source，平时我们会用到KafkaSource以及FileSource,SocketSource、RateSource多用于测试场景。关于输入源没有什么优雅的操作，只能通过重写Source来实现。对于输出源来说，Spark Structured提供的foreach以及foreachBatch已经能适用于大多数场景，没有重写Sink的必要。关于Spark SQL 自定义输入源、Streaming自定义数据源后期会慢慢整理出来。

