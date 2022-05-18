package com.xxx.spark.sparkSQL.sgg

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession, TypedColumn}

/**
 * @author xqh
 * @date 2021/12/31
 * @apiNote
 *
 */
object spark03_UDAF2 {

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


    //早期版本中 spark不能在sql中使用强类型udaf操作

    //早期udaf强类型聚合函数  使用DSL语法操作
    import ss.implicits._
    val ds: Dataset[User] = df.as[User]

    //将UDAF转换为查询列对象
    val udafCol: TypedColumn[User, Long] = new MyAvgUDAF().toColumn

    ds.select(udafCol).show()



  }

  //自定义聚合函数类  计算年龄平均值
  // 强类型的

  /**
   *
   * 3.0版本 强类型  继承 package org.apache.spark.sql.expressions
   * Aggregator
   *
   * IN: 输入类型 改成User
   * BUF:缓冲区类型 Buff
   * OUT:输出
   *
   */
  case class User(name:String,age:Long)
  case class Buff(var total: Long, var count: Long)
  class MyAvgUDAF extends Aggregator[User, Buff, Long] {
    //    初始值 零值
    //    缓冲区初始化
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    //更新缓冲数据
    override def reduce(b: Buff, a: User): Buff = {
      b.total = b.total + a.age
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
