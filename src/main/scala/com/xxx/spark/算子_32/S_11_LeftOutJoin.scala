package com.xxx.spark.算子_32

import org.apache.spark.{SparkConf, SparkContext}

object S_11_LeftOutJoin {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf()
        .setAppName("-----")
        .setMaster("local[7]")
    )
    sc.setLogLevel("warn")

//    11、LeftOutJoin(otherDataSet，numPartitions):左外连接，
    //    包含左RDD的所有数据，如果右边没有与之匹配的用None表示,numPartitions设置分区数，提高作业并行度

    //省略
    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3),("C",1))
    val arr1 = List(("A", "A1"), ("B", "B1"), ("A", "A2"), ("B", "B2"))
    val rdd = sc.parallelize(arr, 3)
    val rdd1 = sc.parallelize(arr1, 3)
    val leftOutJoinRDD = rdd.leftOuterJoin(rdd1)
    leftOutJoinRDD .foreach(println)
    sc.stop

/*    # (B,(2,Some(B1)))
    # (B,(2,Some(B2)))
    # (B,(3,Some(B1)))
    # (B,(3,Some(B2)))
    # (C,(1,None))
    # (A,(1,Some(A1)))
    # (A,(1,Some(A2)))
    # (A,(2,Some(A1)))
    # (A,(2,Some(A2)))
    */






  }

}
