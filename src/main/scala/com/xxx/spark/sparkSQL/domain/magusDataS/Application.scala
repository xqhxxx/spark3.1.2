package com.xxx.spark.sparkSQL.domain.magusDataS

import org.apache.spark.sql.SparkSession

object Application {
  def main(args: Array[String]): Unit = {

    val ss=SparkSession
      .builder()
      .appName("op data ")
      .master("local[*]")
      .getOrCreate()

    val sc=ss.sparkContext
    sc.setLogLevel("warn")

    import ss.implicits._

    val time=Array("2021-12-08 00:00:00","2021-12-08 23:59:59")

    //根据时间 查询magus 计算
    Magus.calculateDaily(ss,sc,time)

  }

}

//magus测点数据
case class MagusColumnType(kks: String, av : Double, tm : Long)
