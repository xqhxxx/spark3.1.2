package com.xxx.spark.算子_32

import org.apache.spark.{SparkConf, SparkContext}

object S_combineByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("-----")
      .setMaster("local[1]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")


    //相同key的数据的平均值
    val rdd = sc.makeRDD(List(
      ("A", 1), ("A", 2), ("b", 3),
      ("b", 4), ("b", 5), ("A", 6)),
      2)

    //    比较aggregateByKey
    //三个参数
    //一：将相同key的第一个数据进行结构转换 类比比较aggregateByKey的第一个参数列表初始值
    //二：分区内规则
    //三：分区间规则

    val newRDD = rdd.combineByKey(
      v => (v, 1),

      (t: (Int, Int), v) => {
        (t._1 + v, t._2 + 1)
      },

      (t1: (Int, Int), t2: (Int, Int)) => {
        (t1._1 + t2._1, t2._2 + t2._2)
      }

    )

    newRDD.mapValues {
      case (num, cnt) => {
        num / cnt
      }

    }.collect().foreach(println)

    sc.stop()

  }

}
