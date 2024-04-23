package com.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: glom
 * @author: JunWen
 * @create: 2024-04-06 20:10
 * */
object Spark05_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    // TODO 算子 - glom
    val rdd: RDD[Int] = sparkContext.makeRDD(List(1, 2, 3, 4), 2)
    // List => Int
    // Int => Array
    val glomRDD: RDD[Array[Int]] = rdd.glom()

    glomRDD.collect().foreach(data => println(data.mkString(",")))
    sparkContext.stop()
  }
}
