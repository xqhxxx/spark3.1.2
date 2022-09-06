import org.apache.spark.sql.SparkSession

/**
 * @author xqh
 * @date 2022/9/6  17:17:48
 * @apiNote
 *
 */
object Main {
  def main(args: Array[String]): Unit = {


    val ss = SparkSession.builder()
      .master("local[*]")
      .appName("hive test rep")
      .getOrCreate()


    val df = ss.read
      .option("header", value = true)
      .csv("datas/hist.csv")


    df.createOrReplaceTempView("hist_tb")
//    df.show(false)

    val df2 = ss.read
      .option("header", value = true)
      .csv("datas/tb.csv")

    df2.createOrReplaceTempView("kks_tb")
//    df2.show(false)

    ss.sql(
      s"select nvl(),av from hist_tb a left join kks_tb b where a.kks=b.kks1 ")

      .show(false)

  }

}
