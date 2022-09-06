package com.xxx.spark.operation_32.transformation

import org.apache.spark.{SparkConf, SparkContext}

object S_19_repartition {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf()
        .setAppName("-----")
        .setMaster("local[8]")
    )
    sc.setLogLevel("warn")


    // 即Reshuffle RDD并随机分区，使各分区数据量尽可能平衡。
    // 若分区之后分区数远大于原分区数，则需要shuffle。
    var rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
//    val value = rdd.coalesce(3,shuffle = true)
//    value.saveAsTextFile("output")

    // repartition是coalesce接口中shuffle为true的简易实现，
    rdd.repartition(3).saveAsTextFile("output")

  }

}
