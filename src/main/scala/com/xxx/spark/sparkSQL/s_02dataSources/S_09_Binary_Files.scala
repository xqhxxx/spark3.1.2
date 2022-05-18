package com.xxx.spark.sparkSQL.s_02dataSources

import org.apache.spark.sql.SparkSession

/**
 * 二进制文件
 */
object S_09_Binary_Files {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      .appName("Binary File Data Source")
      .master("local")
      .getOrCreate()

    val bDF = ss.read
      .format("binaryFile")
      .option("pathGlobFilter", "*.png")
      .load("DataSource/")

    bDF.show()


  }

}
