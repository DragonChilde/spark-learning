package com.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: flatMap
 * @author: JunWen
 * @create: 2024-04-06 17:35
 * */
object Spark04_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    // TODO 算子 - flatMap
    val rdd: RDD[List[Int]] = sparkContext.makeRDD(List(List(1, 2), List(3, 4)))

    val flatRDD = rdd.flatMap(
      list => {
        list
      }
    )
    flatRDD.collect().foreach(println)
    sparkContext.stop()
  }

}
