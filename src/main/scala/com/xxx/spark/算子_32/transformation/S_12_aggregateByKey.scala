package com.xxx.spark.算子_32.transformation

import org.apache.spark.{SparkConf, SparkContext}

object S_12_aggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("-----")
      .setMaster("local[7]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

    //类似reduceByKey，对pairRDD中想用的key值进行聚合操作，
    //reduceByKey 分区内 分区间的聚合规则一样
    // 如果分区 内外 规则不一样：例如：内求最大值 分区间求和
    val rdd = sc.makeRDD(List(("A", 1), ("A", 2), ("A", 3), ("A", 4)), 2)

    //2个参数列表
    // 第一个参数列表,初始值 用于分区内和第一个key做计算
    // 第二个参数列表2个参数
    //    第一个参数分区内规则
    //    第二个参数分区间，
    rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    ).collect().foreach(println)


    ///聚合规则相同的时候
    rdd.foldByKey(0)(_ + _).collect().foreach(println)

    //相同key的数据的平均值
    //(b,3)
    //(A,4)
    val rdd2 = sc.makeRDD(List(
      ("A", 1), ("A", 2), ("b", 3),
      ("b", 4), ("b", 5), ("A", 6)),
      2)

    val newRDD = rdd2.aggregateByKey((0, 0))(
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {
        (t1._1 + t2._1, t2._2 + t2._2)
      }
    )
    newRDD.mapValues {
      case (num, cnt) => {
        num / cnt
      }

    }.collect().foreach(println)



    // 使用初始值（seqOp中使用，而combOpenCL中未使用）对应返回值为pairRDD，
    // 而区于aggregate（返回值为非RDD）

    sc.stop()

  }

}
