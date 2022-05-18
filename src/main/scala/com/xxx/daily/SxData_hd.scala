package com.xxx.daily

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SxData_hd {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession
      .builder()
      .appName("csv data")
      .master("local[1]")
      .getOrCreate()

    val kks_ktp_Path = "C:\\Users\\xqh\\Desktop\\三峡数据核对0214\\sxdxfinal.csv"
    val cy_kks_Path = "C:\\Users\\xqh\\Desktop\\三峡数据核对0214\\抽样测点kks.csv"

    val kks_ktpDF = ss.read
      .option("delimiter", ",")
      .option("header", "true")
      .option("encoding", "gbk")
      .csv(kks_ktp_Path)
    //magus测点数据
    //csv :id         pn      an     rt   ed      ad
    //  麦杰库测点id    kks编码 中文描述 类型 原测点编码 映射编码

    val df4 = kks_ktpDF.select("PN", "AD")
    df4.show(3, false)
    df4.createOrReplaceTempView("t1")
    //
    val cy_kksDF = ss.read
      .option("delimiter", ",")
      .option("header", "true")
      .option("encoding", "gbk")
      .csv(cy_kks_Path)
    //      .selectExpr("_c0 as kks")
    cy_kksDF.show(1)
    cy_kksDF.createOrReplaceTempView("t2")


    val df3: DataFrame = ss.sql(s"select t2.*,t1.ad as ktpCode from t2 left join t1 on t2.kks=t1.pn")
    df3.show(false)
    df3.createOrReplaceTempView("t3")
    println(df3.count())


    df3
      .write.format("csv")
      .option("header", "true")
      .save("output\\cypoint")
    //

    ss.stop()


  }
}



