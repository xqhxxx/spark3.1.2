package com.xxx.spark.算子_32

import org.apache.spark.{SparkConf, SparkContext}

object S_19_case {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(
      new SparkConf()
        .setAppName("-----")
        .setMaster("local[*]")
    )
    sc.setLogLevel("warn")



    //19、case
    val aa=List(1,2,3,"asa")
//    # aa: List[Any] = List(1, 2, 3, asa)
     aa. map {

       case i: Int => i + 1
       case s: String => s.length
       }

//    # res16: List[Int] = List(2, 3, 4, 3)

    //模式匹配
    def matchTest(x: Int): String = x match {
      case 1 => "one"
      case 2 => "two"
      case _ => "many"

    }

  }

}
