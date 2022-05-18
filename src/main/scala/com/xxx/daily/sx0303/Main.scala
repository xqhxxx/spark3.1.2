package com.xxx.daily.sx0303

import com.xxx.magus.MagusUtil
import com.xxx.magus.bean.Points
import com.xxx.spark.sparkSQL.domain.magusDataS.{GetData, MagusColumnType}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}

import java.text.SimpleDateFormat

/**
 * @author xqh
 * @date 2022/3/3
 * @apiNote
 *
 */
object Main {
  def main(args: Array[String]): Unit = {

    val ss = SparkSession
      .builder()
      .appName("get started")
      .master("local[4]")
      .getOrCreate()

    val sc = ss.sparkContext
    sc.setLogLevel("warn")


    import ss.implicits._
    val magusDF: DataFrame = ss.read
      //      .option("header", "true")
      .option("encoding", "UTF-8")
      .csv("C:\\Users\\xqh\\Desktop\\0303三峡数据查点核对\\三峡上导下导水导.csv")
      .selectExpr("_c1 as kks")

    val kks_code: Array[String] = magusDF.map(
      _.getString(0)
    ).collect()


    val time = Array("2021-09-02 00:00:00", "2021-09-02 23:59:59")

    //get data 创建view
    GetData.getByMagus(ss, sc, kks_code, time)

    val df2: DataFrame = ss.sql("select * from tb_magus_hist_data ")

    df2.map(
      x => {
        val kks = x.getString(0)
        (kks, 1)
      }
    ).groupByKey(_._1)
      .count()
      .show(20,false)

//    df2.show(2, false)

  }

  def getByMagus(ss: SparkSession, sc: SparkContext, kks_code: Array[String], time: Array[String]): Unit = {
    import ss.implicits._
    val sdfs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    var startTime = sdfs.parse(time(0) + ".000")
    var endTime = sdfs.parse(time(1) + ".000")

    //    println(startTime)
    //    println(endTime)

    val schema: StructType = StructType(
      Seq(
        StructField("kks", types.StringType, nullable = true),
        StructField("av", types.DoubleType, nullable = true),
        StructField("tm", types.LongType, nullable = true)
      )
    )

    var emptyDf: DataFrame = ss.createDataFrame(sc.emptyRDD[Row], schema)
    var emptyRDD = sc.emptyRDD[Points]
    //todo for循环:  请求java APi数据, union(rdd) ;转换 creat view
    import scala.collection.JavaConverters._
    for (kks <- kks_code) {
      //      println(kks)
      val ori: java.util.List[Points] = MagusUtil.getOPHisData(kks, startTime, endTime)
      val rdd = sc.parallelize(ori.asScala, 1)

      emptyRDD = emptyRDD.union(rdd)
      emptyDf = emptyDf.union(rdd.map(x => {
        MagusColumnType(x.getPointCode, x.getValue.toDouble, x.getlTime())
      }).toDF())
    }

    val magusDF = emptyRDD.map(
      x => {
        MagusColumnType(x.getPointCode, x.getValue.toDouble, x.getlTime())
      }).toDF()

    magusDF.createOrReplaceTempView("tb_magus_hist_data")

    //    ss.sql(" select kks,av,tm from tb_magus_hist_data order by tm asc").show(2)
    ::
    emptyDf.show(5)
  }


}
