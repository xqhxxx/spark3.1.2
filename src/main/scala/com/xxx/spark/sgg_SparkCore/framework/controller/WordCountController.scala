package com.xxx.spark.sgg_SparkCore.framework.controller

import com.xxx.spark.sgg_SparkCore.framework.common.TController
import com.xxx.spark.sgg_SparkCore.framework.service.WordCountService

/**
 * @author xqh
 * @date 2021/12/29
 * @apiNote
 *
 */
class WordCountController extends TController{

  private val wordCountService = new WordCountService()

  //重写调度方法
  def execute(): Unit = {
    //    todo
    val array: Array[(String, Int)] = wordCountService.dataAnalysis()
    array.foreach(println)
  }

}
