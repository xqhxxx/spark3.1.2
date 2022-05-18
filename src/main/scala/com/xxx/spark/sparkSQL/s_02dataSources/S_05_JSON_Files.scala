package com.xxx.spark.sparkSQL.s_02dataSources

import org.apache.spark.sql.SparkSession

object S_05_JSON_Files {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      .appName("Parquet files")
      .master("local")
      .getOrCreate()

    //Spark SQL 可以自动推断 JSON 数据集的模式并将其加载为Dataset[Row].
    // 这种转换是可以做到用SparkSession.read.json()在任一Dataset[String]或JSON文件。
    //请注意，作为json 文件提供的文件不是典型的 JSON 文件。
    // 每行必须包含一个单独的、自包含的有效 JSON 对象。

    import ss.implicits._
    val path="DataSource/people.json"
    ss.read.json(path)
      .printSchema()
  }

}
