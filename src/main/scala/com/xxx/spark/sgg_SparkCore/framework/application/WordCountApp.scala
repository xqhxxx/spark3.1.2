package com.xxx.spark.sgg_SparkCore.framework.application

import com.xxx.spark.sgg_SparkCore.framework.common.TApplication
import com.xxx.spark.sgg_SparkCore.framework.controller.WordCountController

/**
 * @author xqh
 * @date 2021/12/29
 * @apiNote
 *
 */
object WordCountApp extends App with TApplication {

  ///启动
  start() {
    //执行的方案
    val controller = new WordCountController()
    controller.execute()
  }

}
