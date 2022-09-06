package com.xxx.spark.operation_32.transformation

import org.apache.spark.{SparkConf, SparkContext}

object S_13_sortByKey {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf()
        .setAppName("-----")
        .setMaster("local[7]")
    )
    sc.setLogLevel("warn")

    //8、sortByKey(accending，numPartitions):返回以Key排序的（K,V）键值对组成的RDD，
    // accending为true时表示升序，为false时表示降序，numPartitions设置分区数，提高作业并行度。

    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
    val rdd = sc.parallelize(arr)
    val sbkrdd = rdd.sortByKey()
    sbkrdd.foreach(println)
    //    (A,1)
    //    (A,2)
    //    (B,2)
    //    (B,3)

    sc.stop


  }

}
