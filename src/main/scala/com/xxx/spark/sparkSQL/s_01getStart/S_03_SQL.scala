package com.xxx.spark.sparkSQL.s_01getStart

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * 运行sql查询
 */
object S_03_SQL {
  def main(args: Array[String]): Unit = {

    val ss = SparkSession
      .builder()
      .appName("sql")
      .master("local")
      .getOrCreate()

    val dataPath = "D:\\BD\\myProject\\spark3.1.2\\DataSource\\people.json"
    val df = ss.read.json(dataPath)

    //   将 DataFrame 注册为 SQL 临时视图
    df.createOrReplaceTempView("people")

    val sqlDF: DataFrame = ss.sql("select * from people")
    sqlDF.show()

    val rdd: RDD[Row] = sqlDF.rdd
    rdd.foreach(println)

    import ss.implicits._
    val  dt="2021"
    val ds: Dataset[ColumnType] = sqlDF.map {
      x => {
        ColumnType(x.get(0).toString, x.get(1).toString, "sx", dt)
      }
    }
    ds.show()


  }
  case  class ColumnType(age:String,name:String,src:String,dt:String)
}
