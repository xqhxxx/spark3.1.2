package com.xxx.spark.算子_32.transformation

import org.apache.spark.{SparkConf, SparkContext}

object S_09_distinct {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("mapPartitions")

    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

//    返回一个在源数据集去重之后的新数据集，即去重，并局部无序而整体有序返回。（详细介绍见
    //
    //https://blog.csdn.net/Fortuna_i/article/details/81506936）


    //底层是map. reduceByKey (k,null)
  }


}
