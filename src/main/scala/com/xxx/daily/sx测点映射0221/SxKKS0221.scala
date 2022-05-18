package com.xxx.daily.sx测点映射0221

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author xqh
 * @date 2022/2/21
 * @apiNote
 *
 */
object SxKKS0221 {
  def main(args: Array[String]): Unit = {

    val ss = SparkSession
      .builder()
      .appName("kks data")
      .master("local[1]")
      .getOrCreate()

    val p1 = "C:\\Users\\xqh\\Desktop\\三峡kks映射更新0221\\0221_v1.csv"
    val p2 = "C:\\Users\\xqh\\Desktop\\三峡kks映射更新0221\\0221_v2.csv"

        val df1: DataFrame = ss.read
          .option("delimiter", ",")
          .option("header", "true")
          .option("encoding", "gbk")
          .csv(p1)
    //    df1.show(2, false)
        df1.createOrReplaceTempView("t1")
        println(df1.count())

        val df2: DataFrame = ss.read
          .option("delimiter", ",")
          .option("header", "true")
          .option("encoding", "gbk")
          .csv(p2)

    //    df2.show(2, false)
        df2.createOrReplaceTempView("t2")
        println(df2.count())

        val dfT3: DataFrame = ss.sql(s"select t1.*,t2.* from t1 left join t2 on" +
          s" t1.`关联字段`=t2.ENGLISHNAME or t1.`关联字段`=t2.ENGLISHNAME")

        dfT3.show(2,false)
        println(dfT3.count())
    dfT3.createOrReplaceTempView("t3")

        dfT3.coalesce(1)
          .write
          .option("delimiter", ",")
          .option("header", "true")
          .option("encoding", "utf-8")
          .csv("output\\kks")



/*    //todo 处理原测点未匹配上的
    val p3 = "C:\\Users\\xqh\\Desktop\\三峡kks映射更新0221\\t2_ori_code.csv"
    val df3: DataFrame = ss.read
      .option("delimiter", ",")
      .option("header", "true")
      .option("encoding", "utf-8")
      .csv(p3)
        df3.show(2, false)
    df3.createOrReplaceTempView("t3")
//    println(df1.count())*/

/*    val df4: DataFrame = ss.sql("select * from t3 where ENGLISHNAME is null")
    df4.show(2,false)
    println(df4.count())*/

//    df4.createOrReplaceTempView("t4")

//    val df5: DataFrame = ss.sql("select * from t3 left  join t2 on t3.`信号名称`=t2.MEASUREPOINTNAME")
//    println(df5.count())

    ss.read
      .option("delimiter", ",")
      .option("header", "true")
      .option("encoding", "utf-8")
      .csv("C:\\Users\\xqh\\Desktop\\sx测点处理0209\\rtd_kks_tb.csv")
      .createOrReplaceTempView("kks_tb")


//    val df5: DataFrame = ss.sql("select * from kks_tb join t3 on kks_tb.rt=t3.MEASUREPOINTCODE and kks_tb.kks!=t3.kks")
//
//    df5.show(2,false)
//
//    println(df5.count())
    //    ss.stop()


  }

}
