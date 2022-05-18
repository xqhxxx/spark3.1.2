package com.xxx.daily

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SxKKSDassDxAx {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession
      .builder()
      .appName("csv data")
      .master("local[1]")
      .getOrCreate()

    val dassDataPath = "C:\\Users\\xqh\\Desktop\\sx测点处理0209\\DASS_sx\\*"
    val kksDXDataPath = "C:\\Users\\xqh\\Desktop\\sx测点处理0209\\开关量.csv"

    val dassDF = ss.read
      .option("delimiter", ",")
      .option("header", "true")
      .option("encoding", "gbk")
      .csv(dassDataPath)
    //magus测点数据
    //csv :id         pn      an     rt   ed      ad
    //  麦杰库测点id    kks编码 中文描述 类型 原测点编码 映射编码

    val df4 = dassDF.select("ID", "PN", "AN", "RT", "ED", "AD")
    df4.show(3,false)
    df4.createOrReplaceTempView("t1")
//
    val dxDF=ss.read
      .option("delimiter", ",")
      .option("header", "false")
      .option("encoding", "utf-8")
      .csv(kksDXDataPath)
      .selectExpr("_c0 as kks")
    dxDF.show(1)
    dxDF.createOrReplaceTempView("t2")


    val df3: DataFrame = ss.sql(s"select t1.*,t2.kks from t1 left join t2 on t1.pn=t2.kks")
    df3.show(false)
    df3.createOrReplaceTempView("t3")

//ss.sql("update t3 set rt='DX' where kks is null").show(false)

    val df5: DataFrame = df3.withColumn("rt", when(col("kks") isNotNull, "DX").otherwise(col("rt")))
    df5.show(false)

    df5.select("ID", "PN", "AN", "RT", "ED", "AD").show(2,false)

    df5.select("ID", "PN", "AN", "RT", "ED", "AD")
//    df4.coalesce(1)
      .write.format("csv")
      .option("header", "true")
      .save("output\\dxFinalPoint")


    ss.stop()


  }
}



