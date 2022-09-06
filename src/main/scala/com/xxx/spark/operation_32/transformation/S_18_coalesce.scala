package com.xxx.spark.operation_32.transformation

import org.apache.spark.{SparkConf, SparkContext}

object S_18_coalesce {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf()
        .setAppName("coalesce")
        .setMaster("local[*]")
    )
    sc.setLogLevel("warn")

    //   重新分区，减少RDD中分区的数量到numPartitions。

    //缩减分区 提高小数据集的执行效率

    var rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    //默认情况下不会打乱分区数据 12 3456
    //    val value = rdd.coalesce(2)

    //导致数据倾斜，想让均衡。shuffle一下
    //145 236
    val value = rdd.coalesce(2, true)
    value.saveAsTextFile("output")

    sc.stop()


  }

}
