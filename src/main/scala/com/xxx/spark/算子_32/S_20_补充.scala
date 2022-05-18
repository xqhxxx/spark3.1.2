package com.xxx.spark.算子_32

import org.apache.spark.{SparkConf, SparkContext}

object S_20_补充 {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(
      new SparkConf()
        .setAppName("-----")
        .setMaster("local[*]")
    )
    sc.setLogLevel("warn")

    while (true){

      println(sc.makeRDD(Seq(1, 2, 3)).count())

      Thread.sleep(1*1000)
    }
//    sc.stop

    //补充 reduceByKey 与 groupByKey的区别？

    //  1、  reduceByKeyt时，Spark可以在每个分区移动数据之前将待输出数据与一个共用的key结合

    //    2、groupByKey时，由于它不接收函数，spark只能先将所有的键值对(key-value pair)都移动，这样的后果是集群节点之间的开销很大，导致传输延时

    //    因此，在对大数据进行复杂计算时，reduceByKey优于groupByKey。
    //另外，如果仅仅是group处理，那么以下函数应该优先于 groupByKey ：
    //　　（1）combineByKey 组合数据，但是组合之后的数据类型与输入时值的类型不一样。
    //　　（2）foldByKey合并每一个 key 的所有值，在级联函数和“零值”中使用。
    //

  }

}
