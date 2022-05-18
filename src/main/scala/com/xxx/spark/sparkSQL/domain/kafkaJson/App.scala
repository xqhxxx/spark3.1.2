package com.xxx.spark.sparkSQL.domain

import com.alibaba.fastjson.JSON
import com.google.gson.{Gson, JsonObject, JsonParser}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.json4s.jackson.JsonMethods.parse

import scala.:+
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON.headOptionTailToFunList

object App {
  def main(args: Array[String]): Unit = {

    val ss = SparkSession
      .builder()
      .appName("main")
      .master("local[2]")
      .getOrCreate()

    //


    import ss.implicits._

    // source
    val df = ss.readStream
      .format("kafka")
      .options(Map[String, String](
        "kafka.bootstrap.servers" -> "localhost:9092",
        "subscribe" -> "sx_only_test",
      ))
      .load()
      //struct<key:binary,value:binary,topic:string,partition:int,offset:bigint,timestamp:timestamp,timestampType:int>
      //只取key value
      //      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      //      .as[(String, String)]
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    val listPointDS = df.map(
      x => {
        val list = new ListBuffer[Jspoint]
        val jsonArray = JSON.parseArray(x)
        for (i <- 0 until jsonArray.size()) {
          val jsonObject = jsonArray.getJSONObject(i)
          val nb: Jspoint = JSON.parseObject(jsonObject.toString, classOf[Jspoint])
          list.append(nb)
        }
        list
      })
    //    val listPointDS = df.map(x => handleMessage2CaseClass(x))

    val pointDS = listPointDS.flatMap(
      list => {
        list
      })

    //对list处理数据

    //    Array("a_b","c_d","e_f"))
    //    map :Array(("a,b"},("c,d"),"e_f"))
    //    fm:  Array("a,b","c,d","e,f"))

    val value = listPointDS.map(x => x.head)
      .map((_, 1))
      .groupByKey(_._1)
      .count()

    //    value.count()

    value.writeStream
      .format("console")
      .outputMode("complete")
      .start().awaitTermination()

  }

  /**
   * 将JSON解析为class
   *
   * @param jsonStr
   * @return
   */
  def handleMessage2CaseClass(jsonStr: String): ListBuffer[Jspoint] = {
    val list = new ListBuffer[Jspoint]
    val jsonArray = JSON.parseArray(jsonStr)
    for (i <- 0 until jsonArray.size()) {
      val jsonObject = jsonArray.getJSONObject(i)
      val nb: Jspoint = JSON.parseObject(jsonObject.toString, classOf[Jspoint])
      list.append(nb)
    }
    list
  }

}

case class Jspoint(p: String, t: String, v: String, q: String)