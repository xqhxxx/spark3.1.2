package com.xxx.spark.sparkSQL.s_01getStart

import org.apache.spark.sql.SparkSession

/**
 * 数据集
 */
object S_05_Datasets {
  def main(args: Array[String]): Unit = {

    val ss = SparkSession
      .builder()
      .appName("sql")
      .master("local")
      .getOrCreate()

    import ss.implicits._

    val caseClassDS = Seq(Person("tom", 22)).toDS()
    caseClassDS.show()

    //
    val numDS=Seq(1,2,3).toDS()
    numDS.map(_+1).collect().foreach(println)

    //通过定义的类将 DataFrame 转换为 Dataset。映射将按名称完成
    val dataPath = "D:\\BD\\myProject\\spark3.1.2\\DataSource\\people.json"
    val peopleDS = ss.read.json(dataPath).as[Person]
    peopleDS.show()

  }

}

//类必须创建在main外面 或单独文件
case class Person(name: String, age: Long)
