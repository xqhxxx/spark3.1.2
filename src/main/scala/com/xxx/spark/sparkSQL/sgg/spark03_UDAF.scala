package com.xxx.spark.sparkSQL.sgg

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @author xqh
 * @date 2021/12/31
 * @apiNote
 *
 */
object spark03_UDAF {

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

//    //自定义函数
//    ss.udf.register("prefixName", (name: String) => {
//      "Name:" + name
//    })
    ss.sql("select age,name from people").show()
    //加前缀prefixName
//    ss.sql("select age,prefixName(name) from people").show()

    //自定义函数类
    ss.udf.register("ageAvg", new MyAvgUDAF)
    //计算平均年龄
        ss.sql("select ageAvg(age) from people").show()


  }

  //自定义聚合函数类  计算年龄平均值  弱类型的 不推荐用了

  /**
   * 1.继承
   *
   * 2重写 8个方法，
   *
   */

  class MyAvgUDAF extends UserDefinedAggregateFunction {

    //输入数据结构
    override def inputSchema: StructType = {
      StructType(
        Array(
          StructField("age", LongType)
        )
      )
    }

    //缓冲区 数据结构
    override def bufferSchema: StructType = {
      StructType(
        Array(
          StructField("total", LongType),
          StructField("count", LongType)
        )
      )
    }

    //函数计算结果类型
    override def dataType: DataType = LongType

    //函数的稳定性
    override def deterministic: Boolean = true

    //缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      //      buffer(0)=0L
      //      buffer(1)=0L
      buffer.update(0, 0L)
      buffer.update(1, 0L)
    }

    //根据输入的值更新缓冲区的数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0, buffer.getLong(0) + input.getLong(0))
      buffer.update(1, buffer.getLong(1) + 1)
    }

    //缓冲区数据合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
      buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
    }

    //计算  计算平均值
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0) / buffer.getLong(1)
    }
  }

}
