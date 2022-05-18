package com.xxx.spark.算子_32.transformation

import org.apache.spark.{SparkConf, SparkContext}

object S_08_intersection {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("mapPartitions")

    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

//    对于源数据集和其他数据集求交集，并去重，且无序返回。

    var rdd=sc.makeRDD(List(1,2,3,4))
    var rdd2=sc.makeRDD(List(3,4,5,6))

    //交集 3 4
    println(rdd.intersection(rdd2).collect().mkString(","))

    //并集 12343456
    println(rdd.union(rdd2).collect().mkString(","))

    //差集 rdd的角度 12
    println(rdd.subtract(rdd2).collect().mkString(","))

    //拉链 1-3 ,2-4 3-5 4-6
    println(rdd.zip(rdd2).collect().mkString(","))

    //todo ?? 数据类型不一致怎么办
    //交并差集 数据类型要求一致
    // zip可以形成tuple

//    zip 分区数量要保持一致







    sc.stop()



    
  }


}
