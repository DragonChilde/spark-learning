package com.bigdata.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-10 08:59
 * */
object Spark07_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    // TODO 算子 - filter
    val rdd = sparkContext.textFile("data/apache.log")
    rdd.filter(
      line => {
        val data = line.split(" ")
        val time = data(3)
        time.startsWith("17/05/2015")
      }
    ).collect().foreach(println)

    sparkContext.stop()
  }

}
