package com.xxx.spark.operation_32.transformation

import org.apache.spark.{SparkConf, SparkContext}

object S_02_filter {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf()
        .setAppName("-----")
        .setMaster("local[7]")
    )
    sc.setLogLevel("warn")

    //    14、filter() 按条件过滤
    val filterRdd = sc.parallelize(List(1, 2, 3, 4, 5))
      .map(_ * 2)
      .filter(_ > 5)
    filterRdd.collect.foreach(println)
    //    # res5: Array[Int] = Array(6, 8, 10)


  }

}
