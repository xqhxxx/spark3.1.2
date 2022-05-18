package com.xxx.spark.sgg_SparkCore

import org.apache.spark.{SparkConf, SparkContext}

object Spark80_a案例 {
  def main(args: Array[String]): Unit = {


    val sc = new SparkContext(
      new SparkConf()
        .setMaster("local")
        .setAppName(" "))


    var dataRDD = sc.textFile("agent.log")

    //转换
    val mapRDD = dataRDD.map(
      line => {
        val datas = line.split(" ")

        ((datas(1), datas(4)), 1)
      }
    )

    //分组聚合
    val reduceRDD = mapRDD.reduceByKey(_ + _)

    //转换
    val newRDD = reduceRDD.map {
      case ((pre, ad), sum) => {
        (pre, (ad, sum))
      }
    }

    val gRDD = newRDD.groupByKey()

    //组内排序
    val resultRDD = gRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )

    reduceRDD.foreach(println)


  }

}
