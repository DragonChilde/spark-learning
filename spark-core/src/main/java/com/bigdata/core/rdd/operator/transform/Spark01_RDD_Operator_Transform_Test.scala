package com.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-05 19:37
 * */
object Spark01_RDD_Operator_Transform_Test {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    // TODO 算子 - map
    val rdd = sparkContext.textFile("data/apache.log")

    // 长的字符串
    // 短的字符串
    val mapRDD: RDD[String] = rdd.map(
      line => {
        val datas = line.split(" ")
        datas(6)
      }
    )
    mapRDD.collect().foreach(println)
    sparkContext.stop()
  }
}
