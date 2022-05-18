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
object HiveWindowFunTest2 {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .master("local[2]")
      .appName("hive test wf2")
      .getOrCreate()
    val df = ss.read
      .option("header", true)
      .csv("datas/hiveFun2.csv")
    df.createOrReplaceTempView("business")
    //        df.show()


    //todo  模拟hive 测试二
    //1、查询在2017年4月份购买过的顾客及总人数
    //    hiveQuery("select *,count(name)over()as total from business where substr(orderdate,1,7)='2017-04'")

    // 2、查询顾客的购买明细及月购买总额  每月每个用户
    //    hiveQuery("select *,sum(cost)over(partition by name,substr(orderdate,1,7)) as mon_total from business ")

    //3、查询顾客的购买明细及到目前为止每个顾客购买总金额
    //    hiveQuery("select *,sum(cost)over(partition by name order by orderdate rows between unbounded preceding and current row)as total_amount from business ")

    // 4、查询顾客上次的购买时间----lag()over()偏移量分析函数的运用 lag lead
    //    hiveQuery("select *,lag(orderdate,1)over(partition by name order by orderdate) as last_date from business ")

    // 5、查询前20%时间的订单信息
    //ntile(n) 把有序分区中的行分发到指定数据的组中，各个组有编号，编号从1开始，对于每一行，ntile返回此行所属的组的编号
    hiveQuery("select *,from ( ) t where t.sortgroup_num=1")

//    select  *
    //from
    //(select   *,
    //ntile(5)over(order  by  cost)sortgroup_num from  business)t
    //where t.sortgroup_num = 1;


    def hiveQuery(sql: String): Unit = {
      ss.sql(sql).show(truncate = false)
    }

    ss.stop()


  }

}
