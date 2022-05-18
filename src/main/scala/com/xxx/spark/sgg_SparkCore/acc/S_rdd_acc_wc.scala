package com.xxx.spark.sgg_SparkCore.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object S_rdd_acc_wc {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(
      new SparkConf()
        .setAppName("acc")
        .setMaster("local[*]")
    )

    val rdd = sc.makeRDD(List("hello","spark","hello"))

    //使用累加器  避免reduceByKey shuffle过程

    //自定义累加器  创建 注册
    val wcAcc= new MyAccumulator()
    sc.register(wcAcc,"WCAccumulator")

    rdd.foreach(
      word=>{
        //数据累加
        wcAcc.add(word)
      }
    )

    //
    println(wcAcc.value)

    sc.stop()

  }

  /**
   * 自定义数据累加器  wordCount
   *
   * 1 继承AccumulatorV2 定义泛型
   * IN   输入类型
   * OUT  返回类型
   *
   * 2 重写方法
   */
  class MyAccumulator extends AccumulatorV2[String,mutable.Map[String,Long]]{
    private  var wcMap= mutable.Map[String,Long]()

    //判断是否为初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty
    }
//
    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator()
    }

    override def reset(): Unit = {
      wcMap.clear()
    }
//    获取需计算的值
    override def add(word: String): Unit = {
      val newCnt = wcMap.getOrElse(word, 0L) + 1
      wcMap.update(word,newCnt)
    }

//driver合并多个累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1=this.wcMap
      val map2=other.value

      map2.foreach {
        case (word, count) => {
          val newCnt=map1.getOrElse(word,0L)+count
          map1.update(word,newCnt)

        }
      }

    }

//    累加器结果
    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }

}
