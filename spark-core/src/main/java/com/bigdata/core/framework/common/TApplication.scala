package com.bigdata.core.framework.common

import com.bigdata.core.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-30 08:33
 * */
trait TApplication {

  def start(master: String = "local[*]", app: String = "Application")(op: => Unit): Unit = {

    val sparConf = new SparkConf().setMaster(master).setAppName(app)
    val sparkContext = new SparkContext(sparConf)

    EnvUtil.put(sparkContext)

    try {
      op
    } catch {
      case ex => println(ex.getMessage)
    }
    
    sparkContext.stop()
    EnvUtil.clear()

  }

}
