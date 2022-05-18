package com.xxx.spark.sgg_SparkCore.acc

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object S_rdd_Bc {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(
      new SparkConf()
        .setAppName("bc")
        .setMaster("local")
    )

    val rdd1 = sc.makeRDD(List(("a",1),("b",2),("c",3)))
//    val rdd2 = sc.makeRDD(List(("a",4),("b",5),("c",6)))

    //join 或导致数据几何增长  并影响shuffle性能 不推荐

    val map=mutable.Map(("a",4),("b",5),("c",6))
    rdd1.map{
      case (w,c)=>{
        val l = map.getOrElse(w, 0L)
        (w,(c,l))

      }
    }.collect().foreach(println)
//一个executor中闭包   导致大量重复数据
    sc.stop()

  }


}
