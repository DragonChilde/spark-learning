package com.bigdata.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-18 09:54
 * */
object Spark04_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    // TODO - 行动算子
    val rdd = sparkContext.makeRDD(List(1, 1, 1, 4), 2)
    val intToLong: collection.Map[Int, Long] = rdd.countByValue()
    println(intToLong) // Map(4 -> 1, 1 -> 3)

    val rdd2 = sparkContext.makeRDD(
      List(("a", 1), ("a", 2), ("a", 3))
    )
    // 统计每种 key 的个数
    val stringToLong: collection.Map[String, Long] = rdd2.countByKey()
    println(stringToLong) //Map(a -> 3)

    sparkContext.stop()
  }


}
