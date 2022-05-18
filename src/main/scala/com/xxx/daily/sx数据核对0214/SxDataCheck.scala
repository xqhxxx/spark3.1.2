package com.xxx.daily.sx数据核对0214

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author xqh
 * @date 2022/2/16
 * @apiNote
 *
 */
object SxDataCheck {
  def main(args: Array[String]): Unit = {

    val ss = SparkSession
      .builder()
      .appName("check data")
      .master("local[4]")
      .getOrCreate()

    val q3_Path = "D:\\BD\\myProject\\spark3.1.2\\0214CheckData\\checkData_sx3q.csv"
    val hive_Path = "D:\\BD\\myProject\\spark3.1.2\\0214CheckData\\"

    val df1: DataFrame = ss.read
      .option("delimiter", ",")
      .option("header", "false")
      .option("encoding", "gbk")
      .csv(q3_Path)
      //      .selectExpr("_c0 as kks", "_c1 as ktpCode", "_c2 as codeName", "_c3 as tm", "_c4 as av", "_c5 as sys")
      .selectExpr("_c0 as kks", "_c1 as ktpCode", "_c3 as tm", "_c4 as av")
    df1.show(2, false)
    df1.createOrReplaceTempView("t1")
    //|02061MKA06CU015------CAI1INV|MS-0022-SIML-0006-M-003-02|06|0CFB-035|06F：15#瓦受力|2022-02-01 00:30:34:151|-218.0|三峡电站油中气体在线监测系统|

    val df2: DataFrame = ss.read
      .option("delimiter", ",")
      .option("header", "false")
      .option("encoding", "utf-8")
      .csv(hive_Path)
      .selectExpr("_c0 as codename", "_c1 as jz", "_c2 as kks", "_c3 as sys", "_c4 as gn", "_c5 as tm", "_c6 as av", "_c7 as dt")
    //      .selectExpr("_c0 as codename", "_c2 as kks", "_c5 as tm", "_c6 as av", "_c7 as dt")

    df2.show(2, false)
    df2.createOrReplaceTempView("t2")

    //    unix_timestamp('2016-04-08', 'yyyy-MM-dd');
    //    ss.sql("select unix_timestamp('2016-04-08', 'yyyy-MM-dd') ").show()
    //    from_unixtime(0, 'yyyy-MM-dd HH:mm:ss')
    //    ss.sql("select from_unixtime(0, 'yyyy-MM-dd HH:mm:ss') ").show()

    // 时间戳去ms
    ss.sql("select kks,ktpCode,substr(tm,1,19) as tm,av from t1 where tm like '2022-02-01%' ").createOrReplaceTempView("t3")

    //去掉kks前缀 时间戳去ms
    ss.sql("select codename,substr(kks,14) as kks, from_unixtime(substr(tm,1,10), 'yyyy-MM-dd HH:mm:ss') as tm,av,dt from t2").createOrReplaceTempView("t4")


    ss.sql("select * from t3,t4 limit 1").show(false)

    //check
    ss.sql("select count(*) as `三区0201数据统计` from t3  ").show() //618441
    ss.sql("select count(*) as `hive0201数据统计` from t4 ").show() //10806638

    val df: DataFrame = ss.sql("select t3.*,t4.* from t3 join t4 on t3.kks=t4.kks and t3.tm=t4.tm ")
    df.show(2, false)

    println("同kks，tm的数据统计：" + df.count()) //158973


  }

}
