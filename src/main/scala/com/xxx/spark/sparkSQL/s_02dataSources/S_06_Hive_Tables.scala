package com.xxx.spark.sparkSQL.s_02dataSources

import org.apache.spark.sql.SparkSession

import java.io.File

/**
 * hive table
 *
 * 将虚拟机node01上的idea
 */
object S_06_Hive_Tables {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val ss = SparkSession
      .builder()
      .appName("spark hive table")
      .master("local")
      .config("spark.sql.warehouse.dir",warehouseLocation)
//      .enableHiveSupport()
      .getOrCreate()

    //指定hive表的存储格式
    //与不同版本的额hive元数据交互

    // 得配置相关的依赖包、
    //配置文件:hive-site.xml, core-site.xml（用于安全配置）
    // 和hdfs-site.xml（用于 HDFS 配置）文件放在conf/

    //需要向启动 Spark 应用程序的用户授予写入权限

    import ss.sql
//    sql("create database db_test")
    ss.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    ss.sql("LOAD DATA LOCAL INPATH 'DataSources/kv1.txt' INTO TABLE src")

    ss.sql("select * from src").show()



  }

}
