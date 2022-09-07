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
    val df2 = ss.read
      .option("header", value = true)
      .csv("datas/tb.csv")
    df2.createOrReplaceTempView("kks_tb")

    ss.sql(
//      s"select a.kks,nvl(b.kks2,a.kks) as kks_update,av,b.kks1,b.kks2 from hist_tb a left join kks_tb b on a.kks=b.kks1 "

    s"select a.kks, (case when a.kks='k01' then 'k02'  when a.kks='k02' then 'k01' else a.kks end ) as kks_update,av from hist_tb a  "
    )

      .show(false)

  }

}
