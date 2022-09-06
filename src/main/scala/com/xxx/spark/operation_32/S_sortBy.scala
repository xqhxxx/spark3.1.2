package com.xxx.spark.operation_32

import org.apache.spark.{SparkConf, SparkContext}

object S_sortBy {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[8]")
      .setAppName("sample")

    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

    var rdd = sc.makeRDD(List(1, 7, 3, 4, 9, 6, 2, 8, 1), 2)


    rdd.sortBy(x => x).saveAsTextFile("output")

    val arr = sc.makeRDD(List(("A", 1), ("B", 2), ("A", 2), ("B", 3)),2)
    arr.sortBy(t=>t._1,false).collect().foreach(println)


      sc.stop()

  }

}
