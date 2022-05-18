package com.xxx.spark.sparkSQL.sgg

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

/**
 * @author xqh
 * @date 2021/12/31
 * @apiNote
 *
 */
object spark03_UDAF1 {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession
      .builder()
      .config(new SparkConf()
        .setAppName("udf")
        .setMaster("local")
      )
      .getOrCreate()


    val df = ss.read.json("DataSource/people.json")

    df.createOrReplaceTempView("people")

    ss.sql("select age,name from people").show()

    //自定义函数类 强类型
    ss.udf.register("ageAvg", functions.udaf(new MyAvgUDAF))
    //计算平均年龄
    ss.sql("select ageAvg(age) from people").show()

  }

  //自定义聚合函数类  计算年龄平均值
  // 强类型的

  /**
   *
   * 3.0版本 强类型  继承 package org.apache.spark.sql.expressions
   * Aggregator
   *
   * IN: 输入类型
   * BUF:缓冲区类型 Buff
   * OUT:输出
   *
   */
  case class Buff(var total: Long, var count: Long)

  class MyAvgUDAF extends Aggregator[Long, Buff, Long] {
    //    初始值 零值
    //    缓冲区初始化
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    //更新缓冲数据
    override def reduce(b: Buff, a: Long): Buff = {
      b.total = b.total + a
      b.count = b.count + 1
      b
    }

    //合并缓冲区
    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.total = b1.total + b2.total
      b1.count = b1.count + b2.count
      b1
    }

    //计算结果
    override def finish(reduction: Buff): Long = {
      reduction.total / reduction.count
    }

    //  2个固定写法  自定义的类 基类 分别固定的写法
    //缓冲区编码操作
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    //输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}
