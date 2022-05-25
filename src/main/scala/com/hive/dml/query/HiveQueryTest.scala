package com.hive.dml.query

import org.apache.spark.sql.SparkSession

/**
 * @author xqh
 * @date 2022/5/20
 * @apiNote
 *
 */
object HiveQueryTest {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .master("local[1]")
      .getOrCreate()



    ss.sql("")



  }

}
