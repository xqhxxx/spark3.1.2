package com.xxx.spark.sparkSQL.s_01getStart

import org.apache.spark.sql.SparkSession

/**
 * spark sql guide
 * 入门
 *
 */
object S_01_GetStart {

  def main(args: Array[String]): Unit = {

    //
    val ss = SparkSession
      .builder()
      .appName("get started")
      .master("local")
      //      .master("spark://node01:7077")
      //      .config("spark.executor.memory", "2g")
      //      .config("spark.executor.cores", "3")
      //      .config("spark.driver.memory", "1g")
      .getOrCreate()

    val sc = ss.sparkContext
    sc.setLogLevel("warn")

    //创建数据源:  RDD、 hive、Spark Data source
    //todo dataFrames
    //example  json
    val dataPath = "DataSource\\people.json"
    val df = ss.read.json(dataPath)
    df.show()



  }


}
