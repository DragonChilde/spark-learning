package com.bigdata.core.framework.controller

import com.bigdata.core.framework.common.TController
import com.bigdata.core.framework.service.WordCountService

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-30 10:22
 * */
class WordCountController extends TController {

  private val wordCountService = new WordCountService()

  override def dispatch(): Unit = {

    val array = wordCountService.dataAnalysis()
    array.foreach(println)
  }
}
