package com.bigdata.core.framework.util

import org.apache.spark.SparkContext

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-30 08:14
 * */
object EnvUtil {

  private val sparkContextLocal = new ThreadLocal[SparkContext]()

  def put(sc: SparkContext): Unit = {
    sparkContextLocal.set(sc)
  }

  def take(): SparkContext = {
    sparkContextLocal.get()
  }

  def clear(): Unit = {
    sparkContextLocal.remove()
  }


}
