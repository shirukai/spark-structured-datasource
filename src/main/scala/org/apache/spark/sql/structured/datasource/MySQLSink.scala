
package org.apache.spark.sql.structured.datasource

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

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
