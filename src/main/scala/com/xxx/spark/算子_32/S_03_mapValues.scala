package com.xxx.spark.算子_32
import org.apache.spark._
object S_03_mapValues {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("mapvalues")

    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

//3、mapValues(function)
//    原RDD中的Key保持不变，与新的Value一起组成新的RDD中的元素。
    //    因此，该函数只适用于元素为KV对的RDD

    val a = sc.parallelize(List("dog", "tiger", "lion",
      "cat", "panther", " eagle"), 2)
    val b = a.map(x => (x.length, x))
    b.mapValues("x" + _ + "x").collect

/*    # //结果
    # Array(
    # (3,xdogx),
    # (5,xtigerx),
    # (4,xlionx),
    # (3,xcatx),
    # (7,xpantherx),
    # (5,xeaglex)
    # )*/



  }

}
