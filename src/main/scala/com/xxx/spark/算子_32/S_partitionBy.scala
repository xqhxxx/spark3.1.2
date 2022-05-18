package com.xxx.spark.算子_32

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object S_partitionBy {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[8]")
      .setAppName("partitionBy")

    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

    var rdd = sc.makeRDD(List(1, 2, 3, 4),2)

    val mapRDD=rdd.map((_,1))
    //rdd--pairRDDFunction
    //隐式转换--二次编译

    //根据指定分区规则 进行分区
    mapRDD.partitionBy( new HashPartitioner(2))
      .saveAsTextFile("output")

    //分区器如果一样 数量也一样 就什么也不做。
    //
    sc.stop()

  }

}
