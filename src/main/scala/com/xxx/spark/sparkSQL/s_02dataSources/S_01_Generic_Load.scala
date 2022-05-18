package com.xxx.spark.sparkSQL.s_02dataSources

import org.apache.spark.sql.SparkSession

object S_01_Generic_Load {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      .appName("Generic Load/Save Functions")
      .master("local")
      .getOrCreate()

    import ss.implicits._

    //默认数据parquet
    val usersDF = ss.read.load("DataSource/users.parquet")
//    usersDF.show()

    //手动指定数据源
    //如json
    val peopleDF=ss.read.format("json").load("DataSource/people.json")
//    peopleDF.select("name","age").write.format("parquet").save("nameAndAges.parquet")

    val peopleDFCsv=ss.read.format("csv")
      .option("seq",";")
      .option("inferSchema",true)
      .option("header",true)
      .load("DataSource/people.csv")
      .show()

//    usersDF.write.format("orc")
//      .option("orc.bloom.filter.columns", "favorite_color")
//      .option("orc.dictionary.key.threshold", "1.0")
//      .option("orc.column.encoding.direct", "name")
//      .save("users_with_options.orc")

    //直接sql
    val sqlDF = ss.sql(
      "SELECT * FROM parquet.`DataSource/users.parquet`")
      .show()

    //保存模式
//    SaveMode.ErrorIfExists （默认）
//    SaveMode.Append
//    SaveMode.Overwrite
//SaveMode.Ignore


//    保存到持久表 hive元数据
//df.write.option("path", "/some/path").saveAsTable("t")

//    分桶、排序和分区
    //对于基于文件的数据源，还可以对输出进行存储分区和排序或分区。分桶和排序仅适用于持久表：







  }

}
