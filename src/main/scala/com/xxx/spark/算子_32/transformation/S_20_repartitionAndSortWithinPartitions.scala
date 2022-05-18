package com.xxx.spark.算子_32.transformation

import org.apache.spark.{SparkConf, SparkContext}

object S_20_repartitionAndSortWithinPartitions {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf()
        .setAppName("-----")
        .setMaster("local[7]")
    )
    sc.setLogLevel("warn")

    // 该方法根据partitioner对RDD进行分区，
    // 并且在每个结果分区中按key进行排序。，


  }

}
