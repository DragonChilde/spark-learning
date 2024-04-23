package com.bigdata.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: sortBy
 * @author: JunWen
 * @create: 2024-04-10 21:53
 * */
object Spark12_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    // TODO 算子 - sortBy
    val rdd = sparkContext.makeRDD(List(6, 5, 4, 3, 2, 1), 2)

    val newRDD = rdd.sortBy(num => num)

    newRDD.saveAsTextFile("output")


    sparkContext.stop()
  }


}
