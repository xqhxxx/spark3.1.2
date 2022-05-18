package com.xxx.daily

import org.apache.spark.sql.SparkSession

/**
 * 三峡的kks 做映射  kks
 * kks 原测点码
 */
object MagusPoint_SX_ToTXT {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession
      .builder()
      .appName("MagusPoint_SX_ToTXT data")
      .master("local[1]")
      .getOrCreate()

    val dataPath = "C:\\Users\\xqh\\Desktop\\kks最新测点2021-12-23 取自DASS\\dasscode\\DASS_sx\\*"

    val df3 = ss.read
      .option("delimiter", ",")
      .option("header", "true")
      .option("encoding", "gbk")
      .csv(dataPath)

    //magus测点数据
    //csv :id         pn      an     rt   ed      ad
    //  麦杰库测点id    kks编码 中文描述 类型 原测点编码 映射编码

    //只需要kks  映射

    val df4 = df3.select("PN", "AD")
    df4.show(1,truncate = false)
    println(df4.collect().length)

    //用csv 分隔符用\t 就行
//    df4.coalesce(1)
//      .write
//      .format("csv")
//      .option("delimiter", "\t")
//      .save("output\\sxPoint")

    //拼接一行数据，做txt
    import ss.implicits._
    val df5 = df4.map(
      x => {
        x.get(0) + "\t" + x.get(1)
      }
    )
    df5.show(1,truncate = false)


    df5.coalesce(1)
      .write
      .format("text")
      .option("encoding", "utf-8")
      .save("output\\testTxt")

    ss.stop()

  }
}



