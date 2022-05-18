package com.xxx.spark.算子_32.action

import org.apache.spark.{SparkConf, SparkContext}

object S_01_reduce {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf()
        .setAppName("-----")
        .setMaster("local[7]")
    )
    sc.setLogLevel("warn")

    //16、reduce() 将RDD中元素前两个传给输入函数，产生一个新的return值，
    // 新产生的return值与RDD中下一个元素（第三个元素）组成两个元素，
    // 再被传给输入函数，直到最后只有一个值为止。

    var rdd1 = sc.makeRDD(1 to 10, 2)
    //求和
    println(rdd1.reduce(_ + _))

    ///reduceByKey 相对于groupByKey 会提前做reduce操作

    var rdd2 = sc.makeRDD(Array(("A", 0), ("A", 2), ("B", 1), ("B", 2), ("C", 1)))
    //    分项求和
    val tuple = rdd2.reduce(
      (x, y) => {
        (x._1 + y._1, x._2 + y._2)
      })

    println(tuple.toString())


  }

}
