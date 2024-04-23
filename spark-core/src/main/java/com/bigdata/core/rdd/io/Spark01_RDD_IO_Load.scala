package com.bigdata.core.rdd.io

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-23 07:36
 * */
object Spark01_RDD_IO_Load {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    val rdd = sparkContext.textFile("output1")
    println(rdd.collect().mkString(","))

    val rdd2 = sparkContext.objectFile[(String, Int)]("output2")
    println(rdd2.collect().mkString(","))

    val rdd3 = sparkContext.sequenceFile[String, Int]("output3")
    println(rdd3.collect().mkString(","))

    sparkContext.stop()
  }

}
