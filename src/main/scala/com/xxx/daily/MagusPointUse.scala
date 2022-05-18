package com.xxx.daily

import org.apache.spark
import org.apache.spark.sql.{SaveMode, SparkSession}

object MagusPointUse {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession
      .builder()
      .appName("csv data")
      .master("local[1]")
      .getOrCreate()

    val dataPath = "C:\\Users\\xqh\\Desktop\\code\\*"

    val df3 = ss.read
      .option("delimiter", ",")
      .option("header", "true")
      .option("encoding", "UTF-8")
      .csv(dataPath)
    //magus测点数据
    //csv :id         pn      an     rt   ed      ad
    //  麦杰库测点id    kks编码 中文描述 类型 原测点编码 映射编码

    val df4 = df3.select("ID", "PN", "AN","TM")
    df4.show(1)
    println(df4.collect().length)

//    df4.coalesce(1)
//      .write.format("csv")
//      .save("output\\point.csv")



    ss.stop()


  }
}



