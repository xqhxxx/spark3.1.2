package com.xxx.spark.sparkSQL.s_02dataSources

import org.apache.spark.sql.SparkSession

object S_07_JDBC_to_dataBase {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      .appName("spark hive table")
      .master("local")
      .getOrCreate()

    // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
    // Loading data from a JDBC source
    val jdbcDF=ss.read
      .format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/spring-vue")
      .option("dbtable", "user")
      .option("user", "root")
      .option("password", "root")
      .load()

//    jdbcDF.show(2)

    val dbMap=Map(
      "url"->"jdbc:mysql://localhost:3306/spring-vue",
      "dbtable"->"user",
      "user"->"root",
      "password"->"root"
    )
    ss.read
      .format("jdbc")
      .options(dbMap)
      .load().show()

    //sava data to jdbc
//    jdbcDF.write
//      .format("jdbc")
//      .option("url","jdbc:mysql://localhost:3306/spring-vue")
//      .option("dbtable", "user1")
//      .option("user", "root")
//      .option("password", "root")
//      .mode("append")
//      .save()





  }

}
