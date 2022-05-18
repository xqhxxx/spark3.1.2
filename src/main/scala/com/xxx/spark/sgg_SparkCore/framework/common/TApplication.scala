package com.xxx.spark.sgg_SparkCore.framework.common

import com.xxx.spark.sgg_SparkCore.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author xqh
 * @date 2021/12/29
 * @apiNote
 *
 * 公共方法  抽象化
 */
trait TApplication {

  /**
   * @param mastre
   * @param app
   * @param op
   */
  def start(mastre: String = "local[*]", app: String = "App")(op: => Unit): Unit = {

    val sc = new SparkContext(
      new SparkConf()
        .setAppName(app)
        .setMaster(mastre)
    )

    //环境工具  来存取sc
    EnvUtil.put(sc)

    try {
      op
    } catch {
      case ex: Throwable => println(ex.getMessage)
    }

    EnvUtil.clear()

  }
}
