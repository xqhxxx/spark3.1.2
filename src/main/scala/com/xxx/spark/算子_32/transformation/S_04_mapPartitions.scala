package com.xxx.spark.算子_32.transformation

import org.apache.spark.{SparkConf, SparkContext}

object S_04_mapPartitions {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("mapPartitions")

    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

    //    2、mapPartitions(function)
    //    map()的输入函数是应用于RDD中每个元素，
    //    而mapPartitions()的输入函数是应用于每个分区

    val input = sc.parallelize(List(1, 2, 3, 4, 5, 6), 2) //RDD有6个元素，分成2个partition
    val result = input.mapPartitions(
      partition => Iterator(sumOfEveryPartition(partition)))
    //partition是传入的参数，是个list，要求返回也是list，即Iterator(sumOfEveryPartition(partition))
    result.collect().foreach {
      println(_)
      //      # 6 15,分区计算和
    }

    sc.stop()
  }

  def sumOfEveryPartition(input: Iterator[Int]): Int = {
    var total = 0
    input.foreach { elem =>
      total += elem
    }
    total
  }

}
