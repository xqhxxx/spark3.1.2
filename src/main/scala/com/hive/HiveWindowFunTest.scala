package com.hive

import org.apache.spark.sql.SparkSession

/**
 * @author xqh
 * @date 2022/5/18
 * @apiNote
 *
 * https://www.jianshu.com/p/3f3cf58472ca
 *
 */
object HiveWindowFunTest {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .master("local[2]")
      .appName("hive test wf")
      //      .enableHiveSupport()
      .getOrCreate()


    val df = ss.read
      .option("header", true)
      .csv("datas/hiveFun.csv")

    df.createOrReplaceTempView("test_window")
    //    df.show()


    //todo  模拟hive 测试一
    //ss.sql("select  *  from test_window").show()
    //1、使用 over() 函数进行数据统计, 统计每个用户及表中数据的总数
    //    hiveQuery("select *,count(userid) over() as total  from  test_window")

    //2、求用户明细并统计每天的用户总数  partition by 按日期列对数据进行分区处理
    //    hiveQuery("select  *,count(logday)over(partition by logday) as day_total from  test_window;")

    //3、计算从第一天到现在的所有 score 大于80分的用户总数 每天往后累加的
    //只会显示总数
    //    hiveQuery("select *,count(score)over()as total from test_window where score > 80")
    //ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW（表示从起点到当前行）
    //    hiveQuery("select *,count(score)over(order by logday rows between unbounded preceding and current row)as total from  test_window where score > 80;")

    //4、计算每个用户到当前日期分数大于80的天数
    //hiveQuery("select * ,count(userid)over(partition by userid order by logday rows between unbounded preceding and current row) as total from test_window where score > 80 order by logday,userid ")



    def hiveQuery(sql: String): Unit = {
      ss.sql(sql).show(truncate = false)
    }

    ss.stop()


  }

}
