package com.xxx.spark.sparkSQL.domain.magusDataS

import com.xxx.magus.MagusUtil
import com.xxx.magus.bean.Points
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}

import java.text.SimpleDateFormat

/**
 *
 */
object GetData {

  /**
   * 根据测点 时间获取magus数据
   *
   * @param ss
   * @param sc
   * @param kks_code
   * @param time
   */
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
