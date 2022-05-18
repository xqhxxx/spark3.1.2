package com.xxx.spark.sgg_SparkCore.framework.util

import org.apache.spark.SparkContext

/**
 * @author xqh
 * @date 2021/12/29
 * @apiNote
 *
 *
 * 环境工具类。用来存取停 SparkContext
 *
 * 避免耦合
 */
object EnvUtil {
  private val scLocal = new ThreadLocal[SparkContext](
  )


  def put(sc: SparkContext) = {

    scLocal.set(sc)
  }

  def take() = {
    scLocal.get()
  }

  def clear() = {
    scLocal.remove()
  }

}
