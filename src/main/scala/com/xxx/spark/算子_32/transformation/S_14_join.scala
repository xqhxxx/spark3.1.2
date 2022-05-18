package com.xxx.spark.算子_32.transformation

import org.apache.spark.{SparkConf, SparkContext}

object S_14_join {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf()
        .setAppName("-----")
        .setMaster("local[5]")
    )

    //10、join(otherDataSet,numPartitions):对两个RDD先进行cogroup操作形成新的RDD
    // ，再对每个Key下的元素进行笛卡尔积，numP

    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
    val arr1 = List(("A", "A1"), ("B", "B1"), ("A", "A2"), ("B", "B2"))
    val rdd = sc.parallelize(arr, 3)
    val rdd1 = sc.parallelize(arr1, 3)
    val groupByKeyRDD = rdd.join(rdd1)
    groupByKeyRDD.foreach(println)

    //    # (B,(CompactBuffer(2, 3),CompactBuffer(B1, B2)))
    //    # (A,(CompactBuffer(1, 2),CompactBuffer(A1, A2)))

    //笛卡尔积变成下面

    /*
    * (B,(2,B1))
(A,(1,A1))
(B,(2,B2))
(A,(1,A2))
(B,(3,B1))
(A,(2,A1))
(B,(3,B2))
(A,(2,A2))
* */


  }
}
