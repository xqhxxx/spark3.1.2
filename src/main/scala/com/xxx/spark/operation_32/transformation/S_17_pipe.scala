package com.xxx.spark.operation_32.transformation

import org.apache.spark.{SparkConf, SparkContext}

object S_17_pipe {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf()
        .setAppName("-----")
        .setMaster("local[7]")
    )
    sc.setLogLevel("warn")

//    通过一个shell命令来对RDD各分区进行“管道化”。
    //    通过pipe变换将一些shell命令用于Spark中生成的新RDD，



  }

}
