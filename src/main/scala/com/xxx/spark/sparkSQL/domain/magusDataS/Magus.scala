package com.xxx.spark.sparkSQL.domain.magusDataS

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import java.text.SimpleDateFormat

object Magus {

  def calculateDaily(ss: SparkSession, sc: SparkContext, time: Array[String]): Unit = {
    import ss.implicits._
    var startTime = time(0)
    var endTime = time(1)
    val sdfs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")


    //读测点：
    val kksCodeRDD = sc.textFile("src/main/scala/com/xxx/spark/sparkSQL/domain/magusDataS/code.txt")

    val kksRow = kksCodeRDD
      .map(_.split(" "))
      .map(x => Row(x(0), x(1), x(2)))


    //    val schema=StructType(
    //      "node kks oriCode".split(" ")
    //        .map(x=>StructField(x,StringType,nullable = true))
    //    )

    val schema = StructType(
      Seq(
        StructField("node", StringType, nullable = true),
        StructField("kks", StringType, nullable = true),
        StructField("oriCode", StringType, nullable = true)
      )
    )

    val kksDF = ss.createDataFrame(kksRow, schema)
    kksDF.createOrReplaceTempView("tb_kks")

//    kksDF.show()
    val kks_code: Array[String] = kksDF.map(_.getString(1)).collect()


    //get data 创建view
    GetData.getByMagus(ss, sc, kks_code, time)

    //查询需要数据
    val resDF = ss.sql("select kks,av,tm from tb_magus_hist_data order by tm asc")

    resDF.show(2)
    resDF.groupBy("kks").count().show()

    //    resDF.write
    //      .format("jdbc")


  }

}
