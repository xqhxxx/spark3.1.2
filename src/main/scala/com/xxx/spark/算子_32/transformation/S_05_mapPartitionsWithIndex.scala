package com.xxx.spark.算子_32.transformation

import org.apache.spark.{SparkConf, SparkContext}

object S_05_mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("mapPartitions")

    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

//    与mapPartitions类似，但需要提供一个表示分区索引值的整型值作为参数，
    //    因此function必须是（int， Iterator<T>）=>Iterator<U>类型的

  }

}
