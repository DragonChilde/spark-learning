package com.bigdata.core.framework.application

import com.bigdata.core.framework.common.TApplication
import com.bigdata.core.framework.controller.WordCountController

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-30 10:45
 * */
object WordCountApplication extends App with TApplication {

  start() {
    val controller = new WordCountController()
    controller.dispatch()
  }

}
