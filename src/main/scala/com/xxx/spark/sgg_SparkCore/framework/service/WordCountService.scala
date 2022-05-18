package com.xxx.spark.sgg_SparkCore.framework.service

import com.xxx.spark.sgg_SparkCore.framework.common.TService
import com.xxx.spark.sgg_SparkCore.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
 * @author xqh
 * @date 2021/12/29
 * @apiNote
 *
 */
class WordCountService extends TService{

  private val wordCountDao = new WordCountDao

  //数据分析
  def dataAnalysis(): Array[(String, Int)] = {

    val rdd: RDD[String] = wordCountDao.readFile("datas/a.txt")

    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wTo: RDD[(String, Int)] = words.map(word => (word, 1))
    val sum: RDD[(String, Int)] = wTo.reduceByKey(_ + _)
    val array: Array[(String, Int)] = sum.collect()

    array

  }

}

