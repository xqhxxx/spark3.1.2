package com.xxx.spark.算子_32.transformation

import org.apache.spark.{SparkConf, SparkContext}

object S_01_map {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("map")
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

    //    1、map是对RDD中的每个元素都执行一个指定的函数来产生一个新的RDD。
    //    任何原RDD中的元素在新RDD中都有且只有一个元素与之对应。
    val a = sc.parallelize(1 to 9, 3)
    //    # x =>*2是一个函数，x是传入参数即RDD的每个元素，x*2是返回值
    val b = a.map(x => x * 2)
    a.collect
    //    # 结果Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    b.collect
    //    # 结果Array[Int] = Array(2, 4, 6, 8, 10, 12, 14, 16, 18)


    //    list/key--->key-value
    val c = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", " eagle"), 2)
    val d = c.map(x => (x, 1))
    d.collect.foreach(println(_))

    /*
# (dog,1)
# (tiger,1)
# (lion,1)
# (cat,1)
# (panther,1)
# ( eagle,1)
# */

    val l = sc.parallelize(List((1, 'a'), (2, 'b')))
    val ll = l.map(x => (x._1, "PV:" + x._2)).collect()
    ll.foreach(println)
    //    # (1,PVa)
    //    # (2,PVb)


  }


}
