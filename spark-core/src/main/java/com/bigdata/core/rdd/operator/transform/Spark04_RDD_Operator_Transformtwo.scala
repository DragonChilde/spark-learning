package com.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: flatMap
 * @author: JunWen
 * @create: 2024-04-07 11:06
 * */
object Spark04_RDD_Operator_Transformtwo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    // TODO 算子 - flatMap
    val rdd: RDD[String] = sparkContext.makeRDD(List("Hello Scala", "Hello Spark"))

    val flatRDD = rdd.flatMap(
      s => {
        s.split(" ")
      }
    )
    flatRDD.collect().foreach(println)
    sparkContext.stop()
  }
}
