package com.bigdata.core.wc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-18 15:33
 * */
object Spark04_WordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    wordCount(sparkContext)
    sparkContext.stop()
  }

  // groupBy
  def wordCount(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val group = words.groupBy(word => word)
    val wordCount = group.mapValues(iter => iter.size)
    wordCount.collect().foreach(println)
  }

}
