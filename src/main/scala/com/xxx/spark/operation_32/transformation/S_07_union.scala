package com.xxx.spark.operation_32.transformation

import org.apache.spark.{SparkConf, SparkContext}

object S_07_union {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("mapPartitions")

    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

//    对于源数据集和其他数据集求并集，不去重。



  }


}
