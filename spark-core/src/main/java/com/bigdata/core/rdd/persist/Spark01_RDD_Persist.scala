package com.bigdata.core.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-23 18:51
 * */
object Spark01_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val list = List("Hello Scala", "Hello Spark")
    val rdd = sparkContext.makeRDD(list)
    val flatRDD = rdd.flatMap(_.split(" "))
    val mapRDD = flatRDD.map((_, 1))
    val reduceRDD = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println("**************************************")

    val rdd2 = sparkContext.makeRDD(list)
    val flatRDD2 = rdd2.flatMap(_.split(" "))
    val mapRDD2 = flatRDD2.map((_, 1))
    val groupRDD = mapRDD2.groupByKey()
    groupRDD.collect().foreach(println)

    sparkContext.stop()
  }

}
