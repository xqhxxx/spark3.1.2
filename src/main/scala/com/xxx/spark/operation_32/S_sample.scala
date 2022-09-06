package com.xxx.spark.operation_32

import org.apache.spark.{SparkConf, SparkContext}

object S_sample {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[8]")
      .setAppName("sample")

    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

    var rdd=sc.makeRDD(List(1,2,3,4,5,6,7,8,9))
    // 是否放回   （f：抽取几率，t：重复几率）   随机数种子
    println(rdd.sample(true, 0.4)
      .collect().mkString(","))

    sc.stop()

  }

}
