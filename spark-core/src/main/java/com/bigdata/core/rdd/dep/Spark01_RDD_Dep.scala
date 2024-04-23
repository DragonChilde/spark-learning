package com.bigdata.core.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-19 10:19
 * */
object Spark01_RDD_Dep {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val lines: RDD[String] = sparkContext.textFile("data/word.txt")
    println(lines.toDebugString)
    println("*************************")

    val words: RDD[String] = lines.flatMap(_.split(" "))
    println(words.toDebugString)
    println("*************************")

    val wordToOne: RDD[(String, Int)] = words.map((_, 1))
    println(wordToOne.toDebugString)
    println("*************************")

    val wordToSum = wordToOne.reduceByKey(_ + _)
    println(wordToSum.toDebugString)
    println("*************************")

    val array: Array[(String, Int)] = wordToSum.collect()
    array.foreach(println)

    // TODO 关闭连接
    sparkContext.stop()
  }

}
