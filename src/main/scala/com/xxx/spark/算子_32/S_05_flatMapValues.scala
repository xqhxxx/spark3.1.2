package com.xxx.spark.算子_32

import org.apache.spark.{SparkConf, SparkContext}

object S_05_flatMapValues {
  def main(args: Array[String]): Unit = {
    //    Scala的Seq将是Java的List，Scala的List将是Java的LinkedList。

    //      序列Seq、集Set、映射Map
    val conf = new SparkConf()
      .setAppName("-----")
      .setMaster("local[7]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")


    // 5、flatMapValues(function)
    //    Key保持不变，与新的Value一起组成新的RDD中的元素
    val a = sc.parallelize(List((1, 2), (3, 4), (5, 6)))
    val b = a.flatMapValues(x => 1 to x)
    b.collect.foreach(println(_))


    val list = List(("mobin", 22), ("kpop", 20), ("lufei", 23))
    val rdd = sc.parallelize(list)
    //将值扩展成新的值，然后原有的key和新值中的每个元素,组成多个新的k-v元素
    //对比mv： 原有key直接+新值组成个一个k-v值
    val value = rdd.flatMapValues(x => Seq(x, "male"))
    value.foreach(println)
    //如果

  /*  输出：

    (mobin,22)
    (mobin,male)
    (kpop,20)
    (kpop,male)
    (lufei,23)
    (lufei,male)

    如果是mapValues会输出：【对比区别】

    (mobin,List(22, male))
    (kpop,List(20, male))
    (lufei,List(23, male))*/

  }

}
