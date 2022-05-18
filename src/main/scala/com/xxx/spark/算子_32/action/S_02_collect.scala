package com.xxx.spark.算子_32.action

import org.apache.spark.{SparkConf, SparkContext}

object S_02_collect {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf()
        .setAppName("-----")
        .setMaster("local[7]")
    )
    sc.setLogLevel("warn")

    //    15、collect()  会将分布式各个节点上的数据汇聚到一个driver节点上
    //若需要遍历RDD中元素，大可不必使用collect，可以使用foreach语句；
    //若需要打印RDD中元素，可用take语句，返回数据集前n个元素，
    // data.take(1000).foreach(println)，这点官方文档里有说明
    //    单机环境下使用collect问题并不大，但分布式环境下尽量规避

    var rdd1 = sc.makeRDD(1 to 10, 2)
    println(rdd1.getNumPartitions)

    //foreach 直接遍历所有节点上的rdd中的元素
    rdd1.foreach(println)
    // 将数据通过网络将远程数据（分布式环境）拉到本地（本节点），
    rdd1.collect.foreach(println)


  }

}
