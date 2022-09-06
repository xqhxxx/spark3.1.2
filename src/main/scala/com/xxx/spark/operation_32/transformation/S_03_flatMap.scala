package com.xxx.spark.operation_32.transformation

import org.apache.spark.{SparkConf, SparkContext}

object S_03_flatMap {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[8]")
      .setAppName("flatMap")

    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

    //    4、flatMap(function)
    //    与map类似，区别是原RDD中的元素经map处理后只能生成一个元素，
    //    而原RDD中的元素经flatmap处理后可生成多个元素

    val a = sc.parallelize(1 to 3, 2)
    val b = a.flatMap(x => 1 to x)
    //对每个元素扩展
    b.collect.foreach(println(_))
    /*
    结果    Array[Int] = Array( 1,
                               1, 2,
                               1, 2, 3,
                               1, 2, 3, 4)
    */


    //    todo  用模式匹配
    val rdd = sc.makeRDD(List(List(1, 2), 3, List(4, 5)))

    rdd.flatMap(
      data => {
        data match {
          case list: List[_] => list
          case dat=>List(dat)
        }
      }
    ).foreach(println)




  }

}
