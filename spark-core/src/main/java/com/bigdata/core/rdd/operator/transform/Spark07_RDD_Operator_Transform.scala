package com.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: filter
 * @author: JunWen
 * @create: 2024-04-10 08:49
 * */
object Spark07_RDD_Operator_Transform {

def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    // TODO 算子 - filter
    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4))
    val filterRDD: RDD[Int] = rdd.filter(num => num % 2 != 0)
    filterRDD.collect().foreach(println)

    sparkContext.stop()
  }


}
