package com.xxx.spark.operation_32.action

import org.apache.spark.{SparkConf, SparkContext}

object S_aggregate {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf()
        .setAppName("-----")
        .setMaster("local[*]")
    )
    sc.setLogLevel("warn")


    var rdd1 = sc.makeRDD(
      List(1, 2, 3, 4), 2)

    println(rdd1.aggregate(0)(_ + _, _ + _))


    ///fold 简化版agg


    sc.stop
  }

}
