package com.bigdata.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: mapPartitionsWithIndex
 * @author: JunWen
 * @create: 2024-04-06 15:33
 * */
object Spark03_RDD_Operator_Transformtwo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    // TODO 算子 - mapPartitionsWithIndex
    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4))

    val mpiRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        // 1,   2,    3,   4
        //(0,1)(2,2),(4,3),(6,4)
        iter.map(
          num => {
            (index, num)
          }
        )
      }
    )

    mpiRDD.collect().foreach(println)
    sparkContext.stop()
  }

}
