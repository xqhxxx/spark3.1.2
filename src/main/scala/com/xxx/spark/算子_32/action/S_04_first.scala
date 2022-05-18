package com.xxx.spark.算子_32.action

import org.apache.spark.{SparkConf, SparkContext}

object S_04_first {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf()
        .setAppName("-----")
        .setMaster("local[*]")
    )
    sc.setLogLevel("warn")

    //18、first() 返回RDD的第一个元素 类似于take(1)

    var rdd1 = sc.makeRDD(Array(("A", "1"), ("B", "2"), ("C", "3")), 2)

    println(rdd1.first())
    println(rdd1.take(1))


    sc.stop
  }

}
