package com.xxx.spark.sparkSQL.s_03oerformanceTuning

/**
 * 性能调优
 */
object s_01_ {
  def main(args: Array[String]): Unit = {



    //在内存中缓存数据
//    Spark SQL 可以通过调用spark.catalog.cacheTable("tableName")或使用内存中的列格式缓存表dataFrame.cache()。然后 Spark SQL 将只扫描需要的列并自动调整压缩以最小化内存使用和 GC 压力。您可以调用spark.catalog.uncacheTable("tableName")从内存中删除该表。


    //其他配置选项

    //SQL 查询的连接策略提示
//    参考Join Hints

    //SQL 查询的合并提示
//    参考Partitioning Hints的


    //自适应查询执行
    //--合并后洗牌分区
    //--将排序合并连接转换为广播join
    //--优化倾斜连接
  }

}
