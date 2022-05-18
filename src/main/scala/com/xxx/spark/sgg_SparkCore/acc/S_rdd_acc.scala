package com.xxx.spark.sgg_SparkCore.acc

import org.apache.spark.{SparkConf, SparkContext}

object S_rdd_acc {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(
      new SparkConf()
        .setAppName("acc")
        .setMaster("local[*]")
    )

    val rdd = sc.makeRDD(List(1,2,3,4))

    //创建累加器
   val sumACc= sc.longAccumulator("sum")
    rdd.foreach(
      num=>{
        sumACc.add(num)
      }
    )

    //获取累加器的值
    println(sumACc.value)

    sc.stop()

  }

}
