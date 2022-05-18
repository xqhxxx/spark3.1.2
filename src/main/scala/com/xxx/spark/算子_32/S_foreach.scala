package com.xxx.spark.算子_32

import org.apache.spark.{SparkConf, SparkContext}

object S_foreach {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf()
        .setAppName("-----")
        .setMaster("local[*]")
    )

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    //driver端内存集合循环遍历  本地
    rdd.collect().foreach(println)

    println("*******************")
    //executor端内存数据打印  分布式
    rdd.foreach(println)

    /*
    * rdd的方法和scala集合的方法不一样
    * rdd的方法可以发到  分布式计算
    * rdd的方法才叫算子
    *
    * */


    sc.stop()

  }

}
