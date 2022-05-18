package com.xxx.spark.算子_32.transformation

import org.apache.spark.{SparkConf, SparkContext}

object S_16_cartesian {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf()
        .setAppName("-----")
        .setMaster("local[7]")
    )
    sc.setLogLevel("warn")

//    求笛卡尔乘积。该操作不会执行shuffle操作。

//    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
//    val arr1 = List(("A", "A1"), ("B", "B1"), ("A", "A2"), ("B", "B2"))
//    val rdd1 = sc.parallelize(arr, 3)
//    val rdd2 = sc.parallelize(arr1, 3)
//    val groupByKeyRDD = rdd1.cogroup(rdd2)
//    groupByKeyRDD.foreach(println)
//    sc.stop


  }

}
