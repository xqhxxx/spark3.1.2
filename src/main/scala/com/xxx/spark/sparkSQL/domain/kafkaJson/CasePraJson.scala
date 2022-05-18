package com.xxx.spark.sparkSQL.domain

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.google.gson.Gson

import java.util.Objects
import scala.:+
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON.headOptionTailToFunList

/**
 * 样例类  直接 解析json数据
 */
object CasePraJson {
  def main(args: Array[String]): Unit = {

    println("====================样例类解析JSON===========================")
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    implicit val formats = DefaultFormats

    val s5 = """{"db":"test","name":["ren","sss","tang"],"other":{"name":"school","class2":2}}"""


    val r = parse(s5).extract[jsonClass]
    println(r.other.name)

    case class jsonClass(
                          db: String,
                          name: List[String],
                          other: secJsonClass
                        )

    case class secJsonClass(
                             name: String,
                             class2: Int
                           )


    val jsonString = """[ { "p": "m1", "t":"2021-05-25 02:08:12.111", "v": 45.23, "q": 1.0 },{ "p": "m2", "t":"2021-05-25 02:08:12.324", "v": 45.23, "q": 1.0 } ] """

    val jsonArray = JSON.parseArray(jsonString)
    for (i <- 0 until jsonArray.size()) {
      val jsonObject = jsonArray.getJSONObject(i)
      val nb: pointClass = JSON.parseObject(jsonObject.toString, classOf[pointClass])
      println(nb)
    }

    val classes = handleMessage2CaseClass(jsonString)
    println(classes.size)




  }

  def handleMessage2CaseClass(jsonStr: String): ListBuffer[pointClass] = {
    val list =new ListBuffer[pointClass]
    val jsonArray = JSON.parseArray(jsonStr)
    for (i <- 0 until jsonArray.size()) {
      val jsonObject = jsonArray.getJSONObject(i)
      val nb: pointClass = JSON.parseObject(jsonObject.toString, classOf[pointClass])
      list.append(nb)
    }
     list
  }

}

case class pointClass(
                       p: String,
                       t: String,
                       v: String,
                       q: String
                     )
