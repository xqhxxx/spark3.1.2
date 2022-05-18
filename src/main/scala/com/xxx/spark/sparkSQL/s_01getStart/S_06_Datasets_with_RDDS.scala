package com.xxx.spark.sparkSQL.s_01getStart

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * dataset 与rdd转换操作
 */
object S_06_Datasets_with_RDDS {
  def main(args: Array[String]): Unit = {

    val ss = SparkSession
      .builder()
      .appName("sql")
      .master("local")
      .getOrCreate()


    //    2种方式将现有的 RDD 转换为数据集 ：
    //todo  1：反射推断模式
    import ss.implicits._
    val dataPath = "DataSource\\people.txt"
    // file--rdd-df
    val pDF = ss.sparkContext
      .textFile(dataPath)
      .map(_.split(","))
      .map(x => Person(x(0), x(1).trim.toLong))
      .toDF()
    //df -- temp view
    pDF.createOrReplaceTempView("people")
    val teenagersDF = ss.sql("select * from people")
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()

    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
    // Array(Map("name" -> "Justin", "age" -> 19))


    //todo  2：编程方式指定架构
    //当案例类无法提前定义时， 创建步骤
    //    1.从原始RDD创建一个Rows的RDD；
    //    2.创建一个StructType表示的schema，与Step 1中创建的RDD中Rows的结构相匹配。
    //    3.通过SparkSession提供的createDataFrame方法将schema应用到Rows的RDD上

    val peopleRDD = ss.sparkContext
      .textFile(dataPath)
    // 1 rdd--rows
    val rowRDD = peopleRDD.map(_.split(","))
      .map(x => Row(x(0), x(1).trim))

    // 2 schema
    val schemaPeople = "name age"
    val fields = schemaPeople.split(" ")
      .map(x => StructField(x, StringType, nullable = true))
    val schema = StructType(fields)

    // 3 apply schema to rdd
    val ppDF = ss.createDataFrame(rowRDD, schema)

    // Creates a temporary view using the DataFrame
    ppDF.createOrReplaceTempView("people")
    // SQL can be run over a temporary view created using DataFrames
    ss.sql("SELECT name FROM people").show()
  }

}
