package com.xxx.spark.算子_32

import org.apache.spark.{SparkConf, SparkContext}

object S_12_RightOutJoin {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf()
        .setAppName("-----")
        .setMaster("local[7]")
    )
    sc.setLogLevel("warn")

//   12、RightOutJoin(otherDataSet, numPartitions):右外连接，包含右RDD的所有数据，
    //   如果左边没有与之匹配的用None表示,numPartitions设置分区数，提高作业并行度

    //省略
    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
    val arr1 = List(("A", "A1"), ("B", "B1"), ("A", "A2"), ("B", "B2"),("C",1))
    val rdd = sc.parallelize(arr, 3)
    val rdd1 = sc.parallelize(arr1, 3)
    val rightOutJoinRDD = rdd.rightOuterJoin(rdd1)
    rightOutJoinRDD.foreach(println)
    sc.stop

/*
(C,(None,1))
(A,(Some(1),A1))
(B,(Some(2),B1))
(A,(Some(1),A2))
(B,(Some(2),B2))
(A,(Some(2),A1))
(B,(Some(3),B1))
(A,(Some(2),A2))
(B,(Some(3),B2))
    */






  }

}
