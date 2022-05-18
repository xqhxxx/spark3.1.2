package com.xxx.spark.sparkSQL.sgg

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author xqh
 * @date 2021/12/31
 * @apiNote
 *
 */
object spark02_UDF {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession
      .builder()
      .config(new SparkConf()
        .setAppName("udf")
        .setMaster("local")
      )
      .getOrCreate()


    val df = ss.read.json("DataSource/people.json")

    df.createOrReplaceTempView("people")

    //自定义函数
    ss.udf.register("prefixName", (name: String) => {
      "Name:" + name
    })

    //加前缀prefixName
    ss.sql("select age,prefixName(name) from people").show()


  }

}
