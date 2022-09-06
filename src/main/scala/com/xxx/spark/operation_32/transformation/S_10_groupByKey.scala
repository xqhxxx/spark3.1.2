package com.xxx.spark.operation_32.transformation

import org.apache.spark.{SparkConf, SparkContext}

object S_10_groupByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("-----")
      .setMaster("local[7]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

    //7、groupByKey(numPartitions):按Key进行分组，返回[K,Iterable[V]]
    // ，numPartitions设置分区数，提高作业并行度【value并不是累加，而是变成一个数组】

    val arr = List(("A", 1), ("A", 2), ("B", 2), ("B", 3))
    val rdd = sc.parallelize(arr)
    val groupByKeyRDD = rdd.groupByKey()
    groupByKeyRDD.foreach(println)

    //    (B,CompactBuffer(2, 3))
    //    (A,CompactBuffer(1, 2))

    //    # 统计key后面的数组汇总元素的个数
    groupByKeyRDD.mapValues(_.size).foreach(println)
    //    聚合值
    groupByKeyRDD.mapValues(_.toArray.sum).foreach(println)

    //    sc.stop


  }

}
