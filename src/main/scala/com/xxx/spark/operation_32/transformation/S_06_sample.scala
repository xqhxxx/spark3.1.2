package com.xxx.spark.operation_32.transformation

import org.apache.spark.{SparkConf, SparkContext}

object S_06_sample {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("mapPartitions")

    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

    //采样操作，用于从样本中取出部分数据。
    // withReplacement是否放回，fraction采样比例，
    // seed用于指定的随机数生成器的种子。
    // （是否放回抽样分true和false，fraction取样比例为(0, 1]。
    // seed种子为整型实数。）

  }

}
