package com.bigdata.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-06 14:12
 * */
object Spark02_RDD_Operator_Transform_Test {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    // TODO 算子 - mapPartitions
    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4), 2)

    val mapRDD = rdd.mapPartitions(
      iter => {
        List(iter.max).iterator
      }
    )
    mapRDD.collect().foreach(println)
    sparkContext.stop()
  }

}
