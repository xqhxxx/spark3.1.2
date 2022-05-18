package com.xxx.spark.算子_32

import org.apache.spark.{SparkConf, SparkContext}

object S_13_lookup {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf()
        .setAppName("-----")
        .setMaster("local[7]")
    )
    sc.setLogLevel("warn")

//  13、lookup（） lookup用于(K,V)类型的RDD，指定K值，
//  返回RDD中该K对应的所有V值。返回一个WrappedArray【包装类数组】
    var rdd1=sc.parallelize(List((1,"a"),(2,"b"),(3,"c"),(1,"a1")))
    //# rdd1: org.apache.spark.rdd.RDD[(Int, String)] = ParallelCollectionRDD[81] at parallelize at
    val wa = rdd1.lookup(1)
    wa.foreach(println)
    //# res34: Seq[String] = WrappedArray(a)
    //






  }

}
