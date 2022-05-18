package com.xxx.spark.sgg_SparkCore.framework.common

import com.xxx.spark.sgg_SparkCore.framework.util.EnvUtil

/**
 * @author xqh
 * @date 2021/12/29
 * @apiNote
 *
 */
trait TDao {
  def readFile(path:String) = {
    EnvUtil.take().textFile(path)
  }

}
