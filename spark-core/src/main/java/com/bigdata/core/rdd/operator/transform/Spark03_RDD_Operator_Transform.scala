package com.bigdata.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: mapPartitionsWithIndex
 * @author: JunWen
 * @create: 2024-04-06 15:18
 * */
object Spark03_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    // TODO 算子 - mapPartitionsWithIndex
    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4), 2)

    val mpiRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter
        } else {
          Nil.iterator
        }
      }
    )


    mpiRDD.collect().foreach(println)
    sparkContext.stop()
  }

}
