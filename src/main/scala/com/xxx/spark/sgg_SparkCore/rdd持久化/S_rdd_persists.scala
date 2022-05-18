package com.xxx.spark.sgg_SparkCore.rdd持久化

import org.apache.spark.{SparkConf, SparkContext}

object S_rdd_persists {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(
      new SparkConf()
        .setAppName("-----")
        .setMaster("local[*]")
    )

    val rdd = sc.makeRDD(List("hello word", "hello scala", "hello", "spark"))

    val fRdd = rdd.flatMap(_.split(" "))

    val mRdd = fRdd.map(
      x => {
        println("***********")
        (x, 1)
      }
    )

    //持久化数据
    // rdd对象可以重用 数据无法重用 不然rdd重新计算数据
    mRdd.cache()  //默认保存到内存
//    mRdd.persist(StorageLevel.DISK_ONLY)  更改位置

    //action算子执行 才会缓存

    /////checkpoint 需要落盘 需要指定 检查点保存路径
    //保存的文件 不会被删除
//    sc.setCheckpointDir("")
//mRdd.checkpoint()


    mRdd.reduceByKey(_+_).collect().foreach(println)

    println("////////////////////")
    mRdd.groupByKey().collect()foreach(println)




    sc.stop()

  }

}
