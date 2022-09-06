package com.xxx.spark.operation_32.action

import org.apache.spark.{SparkConf, SparkContext}

object S_countByKey {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf()
        .setAppName("-----")
        .setMaster("local[*]")
    )
    sc.setLogLevel("warn")

//    var rdd1 = sc.makeRDD(
//      List(1, 2, 3, 4), 2)
//
//    //统计val出现次数
//    println(rdd1.countByValue())
//

    //统计key出现次数
    //    println(rdd1.countByKey())




    sc.stop
  }

}
