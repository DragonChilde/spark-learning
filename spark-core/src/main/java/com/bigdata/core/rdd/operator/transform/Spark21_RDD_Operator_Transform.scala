package com.bigdata.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: leftOuterJoin
 * @author: JunWen
 * @create: 2024-04-17 07:43
 * */
object Spark21_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    // TODO 算子 - leftOuterJoin
    val rdd1 = sparkContext.makeRDD(
      List(
        ("a", 1), ("a", 2)
      )
    )
    val rdd2 = sparkContext.makeRDD(
      List(
        ("a", 4), ("b", 5), ("c", 6)
      )
    )

    val leftJoinRDD = rdd1.leftOuterJoin(rdd2)
    leftJoinRDD.collect().foreach(println)
    /*
    (a,(1,Some(4)))
    (a,(2,Some(4)))
     */

    val rightJoinRDD = rdd1.rightOuterJoin(rdd2)
    rightJoinRDD.collect().foreach(println)
    /*
    (b,(None,5))
    (c,(None,6))
     */

    sparkContext.stop()
  }


}
