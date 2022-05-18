package com.xxx.spark.sparkSQL.s_02dataSources

import org.apache.spark.sql.SparkSession

/**
 * Parquet文件
 * 一种列格式，许多其他数据处理系统都支持它。
 * Spark SQL 支持读取和写入 Parquet 文件
 */
object S_03_Parquet_Files {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      .appName("Parquet files")
      .master("local")
      .getOrCreate()


    //todo 1 以编程方式加载数据
    import ss.implicits._
    val peopleDF = ss.read.json("DataSource/people.json")
//    peopleDF.write.parquet("DataSource/people.parquet")

    val parquetFileDF = ss.read.parquet("DataSource/people.parquet")

    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = ss.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
    namesDF.map(attributes => "Name: " + attributes(0)).show()

    //todo 2分区发现

    //模式合并
    //Hive Metastore Parquet 表转换
    //Hive/Parquet 架构协调
    //元数据刷新
    //配置

  }

}
