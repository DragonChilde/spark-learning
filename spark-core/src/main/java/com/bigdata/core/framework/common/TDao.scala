package com.bigdata.core.framework.common

import com.bigdata.core.framework.util.EnvUtil

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-30 09:53
 * */
trait TDao {

  def readFile(path: String) = {
    EnvUtil.take().textFile(path)
  }
}
