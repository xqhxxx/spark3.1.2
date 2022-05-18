package com.xxx.spark.算子_32

import org.apache.spark.SparkContext

object 算子_函数 {
  def main(args: Array[String]): Unit = {

    /**
     * Spark 算子大致可以分为以下两类:
     *
     * Transformation 变换/转换算子：这种变换并不触发提交作业，完成作业中间过程处理。
     * Transformation 操作是延迟计算的，也就是说从一个RDD 转换生成另一个 RDD 的转换操作不是马上执行，需要等到有 Action 操作的时候才会真正触发运算。
     * Action 行动算子：这类算子会触发 SparkContext 提交 Job 作业。
     * Action 算子会触发 Spark 提交作业（Job），并将数据输出 Spark系统。
     */

    /**
     * Transformation算子：分为 Value数据类型 、Key-Value数据类型
     */

    val sc = new SparkContext

    // 1、map是对RDD中的每个元素都执行一个指定的函数来产生一个新的RDD。
    // 任何原RDD中的元素在新RDD中都有且只有一个元素与之对应。
    //    val a: Array[Int] = Array.range(1, 9, 1)
    //    val b = a.map(x => x * 2)
    //    b.foreach(println(_))

    //list/key--->key-value
    //    val  a=List("dog", "tiger", "lion", "cat", "panther", " eagle")
    //    val b = a.map(x => (x, 1))
    //   b.foreach(println(_))

    val l = List((1, 'a'), (2, 'b'))
    var ll = l.map(x => (x._1, "PV:" + x._2))
    ll.foreach(println)

    // 2、mapPartitions(function)
    //    map()的输入函数是应用于RDD中每个元素，而mapPartitions()的输入函数是应用于每个分区

    //    3、mapValues(function)
    //    原RDD中的Key保持不变，与新的Value一起组成新的RDD中的元素。因此，该函数只适用于元素为KV对的RDD
    //    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", " eagle"), 2)
    //    val b = a.map(x => (x.length, x))
    //    b.mapValues("x" + _ + "x").collect
    //    # //结果
    //    # Array(
    //    # (3,xdogx),
    //    # (5,xtigerx),
    //    # (4,xlionx),
    //    # (3,xcatx),
    //    # (7,xpantherx),
    //    # (5,xeaglex)
    //    # )

    //    4、flatMap(function)
    //    与map类似，区别是原RDD中的元素经map处理后只能生成一个元素，而原RDD中的元素经flatmap处理后可生成多个元素
    //    val a = sc.parallelize(1 to 4, 2)
    //    val b = a.flatMap(x => 1 to x) //每个元素扩展
    //    b.collect

    //   5、flatMapValues(function)  key不变 +v组合新的元素
    val a = sc.parallelize(List((1, 2), (3, 4), (5, 6)))
    val b = a.flatMapValues(x => 1 to x)
    b.collect.foreach(println(_))
    /*结果
    (1,1)
    (1,2)
    (3,1)
    (3,2)
    (3,3)
    (3,4)
    (5,1)
    (5,2)
    (5,3)
    (5,4)
    (5,5)
    (5,6)
    */


  }

}
