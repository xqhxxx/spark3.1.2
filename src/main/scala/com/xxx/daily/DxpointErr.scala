package com.xxx.daily

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * @author xqh
 * @date 2022/2/11
 * @apiNote
 *
 */
object  DxpointErr{

  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      .appName("csv data")
      .master("local[1]")
      .getOrCreate()

    val dataPath = "D:\\BD\\myProject\\spark3.1.2\\datas\\error.log"

    val df: Dataset[String] = ss.read
      .textFile(dataPath)

    import ss.implicits._
    df.show(2,false)

    println(df.count())
    val ds2: Dataset[String] = df.flatMap(
      x => {
        x.split("\t")
      }
    )

    println(ds2.count())
    ds2.write.csv("dxerr")

//    ds2.show(10,false)
  }
}
