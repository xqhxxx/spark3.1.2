package com.xxx.spark.sparkSQL.s_01getStart

import org.apache.spark.sql.SparkSession

/**
 * 全局临时视图
 */
object S_04_Global_Temp_VIew {
  def main(args: Array[String]): Unit = {

    val ss=SparkSession
      .builder()
      .appName("sql")
      .master("local")
      .getOrCreate()

    val dataPath = "D:\\BD\\myProject\\spark3.1.2\\DataSource\\people.json"
    val df = ss.read.json(dataPath)

//   Spark SQL 中的临时视图是会话范围的
//    在所有会话之间共享的临时视图,可以创建一个全局临时视图 global_temp.
    df.createGlobalTempView("people")

    val sqlDF=ss.sql("select * from global_temp.people")
    sqlDF.show()

// 跨会话
ss.newSession().sql("select * from global_temp.people").show(1)



  }

}
