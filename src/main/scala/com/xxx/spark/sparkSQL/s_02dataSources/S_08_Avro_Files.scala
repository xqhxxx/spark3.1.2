package com.xxx.spark.sparkSQL.s_02dataSources

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions._

import java.nio.file.{Files, Paths}

/**
 * avro
 * spark-avro模块是外部的
 */
object S_08_Avro_Files {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      .appName("avro")
      .master("local")
      .getOrCreate()

//    //load save
//    val usersDF = ss.read
//      .format("avro")
//      .load("DataSource/users.avro")
//    usersDF.show()
////    usersDF.select("name", "favorite_color")
////      .write.format("avro")
////      .save("namesAndFavColors.avro")

  /**/
    //to_avro() and from_avro()
    // `from_avro` requires Avro schema in JSON string format.
    val jsonFormatSchema = new String(Files.readAllBytes(Paths.get("./DataSources/user.avsc")))
   import ss.implicits._
    val df = ss
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1")
      .load()

    // 1. Decode the Avro data into a struct;
    // 2. Filter by column `favorite_color`;
    // 3. Encode the column `name` in Avro format.
    val output = df
      .select(from_avro('value, jsonFormatSchema) as 'user)
      .where("user.favorite_color == \"red\"")
      .select(to_avro($"user.name") as 'value)

    val query = output
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("topic", "topic2")
      .start()
  }

}
