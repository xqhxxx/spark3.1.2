package com.hive.hwf

import org.apache.spark.sql.SparkSession

/**
 * @author xqh
 * @date 2022/5/18
 * @apiNote
 *
 * https://www.jianshu.com/p/3f3cf58472ca
 *
 */
object HiveWindowFunTest3 {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .master("local[2]")
      .appName("hive test wf3")
      .getOrCreate()
    val df = ss.read
      .option("header", true)
      .csv("datas/hiveFun3.csv")
    df.createOrReplaceTempView("score")
    //        df.show()

    //    row_number()   over(partition by ... order by ...)：排序1-2-3-4-5
    //rank()         over(partition by ... order by ...)：排序 1-1-3
    //dense_rank()   over(partition by ... order by ...)：排序 1-1-2-3
    //count() over(partition by ... order by ...)：求分组后的总数。
    //max()   over(partition by ... order by ...)：求分组后的最大值。
    //min()   over(partition by ... order by ...)：求分组后的最小值。
    //avg()   over(partition by ... order by ...)：求分组后的平均值。
    //lag(field, num, defaultvalue)   over(partition by ... order by ...)：取出前第n行数据。　
    //#field需要查找的字段，num往前查找第num行的数据，defaultvalue没有符合条件的默认值　
    //lead(field, num, defaultvalue)  over(partition by ... order by ...)：取出后n行数据。
    //#field需要查找的字段，num往后查找第num行的数据，defaultvalue没有符合条件的默认值
    //ratio_to_report()   over(partition by ... order by ...)：Ratio_to_report() 括号中就是分子，over() 括号中就是分母。
    //percent_rank()      over(partition by ... order by ...)


    //todo  模拟hive 测试三
    //  row_number()按照值排序时产生一个自增编号，不会重复（如：1、2、3、4、5、6）
    //  rank() 按照值排序时产生一个自增编号，值相等时会重复，会产生空位（如：1、2、3、3、3、6）
    //  dense_rank() 按照值排序时产生一个自增编号，值相等时会重复，不会产生空位（如：1、2、3、3、3、4）


    //1、每门学科学生成绩排名(是否并列排名、空位排名三种实现)
    //    hiveQuery(
    //      "select *,row_number()over(partition by subject order by score desc) as w_1," +
    //      "rank()over(partition by subject order by score desc) as w_2," +
    //      "dense_rank()over(partition by subject order by score desc) as w_3 " +
    //      "from score")


    //2、每门学科成绩排名top n的学生
    hiveQuery("select subject,name,score,topN from (select *,row_number()over(partition by subject order by score desc) topN from score) t where t.topN<=3")


    /**
     *
     * @param sql
     */
    def hiveQuery(sql: String): Unit = {
      ss.sql(sql).show(truncate = false)
    }

    ss.stop()


  }

}
