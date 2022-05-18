package com.xxx.spark.structredStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window

import java.sql.Timestamp

/**
 *
 * 基于 Spark SQL(关系查询处理结构化数据 DS DF)引擎构建的可扩展且容错的流处理引擎
 * 入口函数：SparkSession
 *
 */
object QuickExample {
  def main(args: Array[String]): Unit = {

    val ss = SparkSession
      .builder()
      .appName("QuickExample")
      .master("local[*]")
      .getOrCreate()

    ss.sparkContext.setLogLevel("warn")

    import ss.implicits._

    // 读数据、转换、处理

    //     windows 先启动 nc -L -p 6666
    val l = ss.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 6666)
      .option("includeTimestamp", true)
      .load()

    /*    val f = l.as[String]
          .flatMap(_.split(" "))
          .groupBy("value")
          .count()*/

    val words = l.as[(String, Timestamp)]
      .flatMap(line =>
        line._1.split(" ").map(
          word => (word, line._2))
      ).toDF("word", "timestamp")

    // Group the data by window and word and compute the count of each group
    val f = words.groupBy(
      window($"timestamp", "5 seconds", "1 seconds")
      , $"word"
    ).count().orderBy("window")


    val query = f.writeStream
      .outputMode("Complete")
      .format("console")
      .start()

    query.awaitTermination()

  }

}
