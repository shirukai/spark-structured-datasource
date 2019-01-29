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
