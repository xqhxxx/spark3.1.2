package com.xxx.spark.sgg_SparkCore.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object S_rdd_Bc_02 {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(
      new SparkConf()
        .setAppName("bc")
        .setMaster("local")
    )

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

    val map: mutable.Map[String, Int] = mutable.Map(("a", 4), ("b", 5), ("c", 6))
    //    封装广播变量
    val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

    rdd1.map {
      case (w, c) => {
        //        方位广播变量
        val l = bc.value.getOrElse(w, 0L)
        (w, (c, l))

      }
    }.collect().foreach(println)
    //一个executor中闭包   导致大量重复数据
    sc.stop()

  }


}
