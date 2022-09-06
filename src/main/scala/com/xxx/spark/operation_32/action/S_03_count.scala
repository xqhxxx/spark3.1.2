package com.xxx.spark.operation_32.action

import org.apache.spark.{SparkConf, SparkContext}

object S_03_count {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf()
        .setAppName("-----")
//        .setMaster("spark://node01:7077")
        .setMaster("local")
    )
    sc.setLogLevel("warn")

    //03、count() 统计RDD中元素的个数。
    var rdd1 = sc.makeRDD(Array(("A", "1"), ("B", "2"), ("C", "3")), 2)
    //    # rdd1: org.apache.spark.rdd.RDD[(String, String)] = ParallelCollectionRDD[34] at makeRDD at :21

    val c = rdd1.count //3个
    println(c)

    //countByKey 与count类似，但是是以key为单位进行统计。 返回的是一个map
    println(rdd1.countByKey()) //Map(B -> 1, A -> 1, C -> 1)

    //countByValue  统计一个RDD中各个value的出现次数。
    // 返回一个map，map的key是元素的值，value是出现的次数。
    println(rdd1.countByValue()) //Map((B,2) -> 1, (C,3) -> 1, (A,1) -> 1)
    sc.stop
  }

}
