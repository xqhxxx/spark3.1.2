package com.xxx.spark.sparkSQL.s_01getStart

import org.apache.spark.sql.SparkSession

/**
 * 无类型数据集操作--dataframe操作
 */
object S_02_DataFrame {
  def main(args: Array[String]): Unit = {

    val ss=SparkSession
      .builder()
      .appName("dataframe")
      .master("local")
      .getOrCreate()


    //数据集rdd：--java对象的集合
    //无类型数据集dataframe： rows对象的集合 类似关系型数据库的表 但是对每个的具体数据的类型不清楚

    //dataframe=一些通用dataSet[Row]
    // DataFrame只是知道字段，但是不知道字段的类型，
    // 所以在执行这些操作的时候是没办法在编译的时候检查是否类型失败的，
    // 比如你可以对一个String进行减法操作，在执行的时候才报错，
    // 而DataSet不仅仅知道字段，而且知道字段类型，所以有更严格的错误检查。就跟JSON对象和类对象之间的类比。


    val dataPath = "D:\\BD\\myProject\\spark3.1.2\\DataSource\\people.json"
    val df = ss.read.json(dataPath)

//    使用 Datasets 进行结构化数据处理的基本示例
    import  ss.implicits._

    // Print the schema in a tree format
    df.printSchema()

    // Select only the "name" column
    df.select("name").show()

    // Select everybody, but increment the age by 1
    df.select($"name", $"age" + 1).show()

    // Select people older than 21
    df.filter($"age" > 21).show()

    // Count people by age
    df.groupBy("age").count().show()




  }

}
