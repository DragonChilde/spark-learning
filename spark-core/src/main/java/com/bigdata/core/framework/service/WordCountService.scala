package com.bigdata.core.framework.service

import com.bigdata.core.framework.common.TService
import com.bigdata.core.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-30 10:14
 * */
class WordCountService extends TService {

  private val wordCountDao = new WordCountDao()

  def dataAnalysis() = {
    val lines = wordCountDao.readFile("data/word.txt")
    val words = lines.flatMap(_.split(" "))
    val wordToOne = words.map(word => (word, 1))
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    val array: Array[(String, Int)] = wordToSum.collect()
    array

  }

}
