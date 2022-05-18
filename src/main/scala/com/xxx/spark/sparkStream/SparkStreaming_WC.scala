package com.xxx.spark.sparkStream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author xqh
 * @date 2022/1/21
 * @apiNote
 *
 *
 * 准实时  微批次
 */
object SparkStreaming_WC {
  def main(args: Array[String]): Unit = {

    //第二个参数是采集周期  Durations
    val ssc = new StreamingContext(
      new SparkConf()
        .setMaster("local[*]")
        .setAppName("ssc wc demo")

      ,
            Seconds(3)
//      milliseconds(1000)
    )


    //todo  nc -l -p 9999
    val lines: ReceiverInputDStream[String] =
      ssc.socketTextStream("localhost", 6666)

    val ws: DStream[String] = lines.flatMap(_.split(" "))

    val value: DStream[(String, Int)] = ws.map((_, 1)).reduceByKey(_ + _)

    value.print(10)
    println("////")

    //
//    ssc.stop()

    //启动采集器  等待
    ssc.start()
    ssc.awaitTermination()

  }

}
