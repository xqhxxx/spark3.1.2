package com.xxx.spark.算子_32.transformation

import org.apache.spark.{SparkConf, SparkContext}

object S_11_reduceByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("-----")
      .setMaster("local[7]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

    //    6、reduceByKey(func,numPartitions):按Key进行分组，
    //    使用给定的func函数聚合value值, numPartitions设置分区数，提高作业并行度

    val arr = List(("A", 3), ("A", 2), ("B", 1), ("B", 3))
    val rdd = sc.parallelize(arr)
    //    val reduceByKeyRDD = rdd.reduceByKey(_ + _)
    val reduceByKeyRDD = rdd.reduceByKey((x, y) => x + y)
    reduceByKeyRDD.foreach(println)

    sc.stop

    //    # (A,5)
    //    # (B,4)

  }

}
