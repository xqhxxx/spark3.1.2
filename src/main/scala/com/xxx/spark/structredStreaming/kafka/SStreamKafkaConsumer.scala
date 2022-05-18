package com.xxx.spark.structredStreaming.kafka

import org.apache.spark.sql.SparkSession

object SStreamKafkaConsumer {
  def main(args: Array[String]): Unit = {

    val ss = SparkSession
      .builder()
      .appName("kafka source ")
      .master("local[8]")
      .getOrCreate()

    ss.sparkContext.setLogLevel("warn")

    import ss.implicits._

    // source
    val df = ss.readStream
      .format("kafka")
      .options(Map[String, String](
        "kafka.bootstrap.servers" -> "localhost:9092",
        "subscribe" -> "kafka_test_topic"
      ))
      .load()
      //struct<key:binary,value:binary,topic:string,partition:int,offset:bigint,timestamp:timestamp,timestampType:int>
      //只取key value
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]


    //key:value  value 才是数据
    val words = df
      .flatMap(_._2.split(","))
      //      .map((_, 1))
      //      .groupByKey(_._1)
      //      .count()
      .groupBy("value")
      .count()

    //
    val query = words
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }

}
