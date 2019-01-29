package org.apache.spark.sql.structured.datasource

import java.util.Properties

import com.mchange.v2.c3p0.ComboPooledDataSource

/**
  * @author : shirukai
  * @date : 2019-01-25 11:24
  */
object C3p0Utils {
  def getDataSource(dbOptions: Map[String, String]): ComboPooledDataSource
  = {
    val properties = new Properties()
    dbOptions.foreach(x => properties.setProperty(x._1, x._2))
    val dataSource = new ComboPooledDataSource()
    dataSource.setDriverClass(dbOptions("driverClass"))
    dataSource.setJdbcUrl(dbOptions("jdbcUrl"))
    dataSource.setProperties(properties)
    dataSource
  }

}
