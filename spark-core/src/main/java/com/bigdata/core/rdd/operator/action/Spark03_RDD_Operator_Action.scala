package com.bigdata.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-18 09:35
 * */
object Spark03_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)
    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4), 2)

    // TODO - 行动算子
    // 将该 RDD 所有元素相加得到结果
    // 10 + ( 1 + 2 + 10) + (3 + 4 +10)
    //10 + 13 + 17 = 40
    // aggregateByKey : 初始值只会参与分区内计算
    // aggregate : 初始值会参与分区内计算,并且和参与分区间计算
    val result = rdd.aggregate(10)(_ + _, _ + _)
    println(result) //40

    val result2 = rdd.fold(10)(_ + _)
    println(result2) //40

    sparkContext.stop()
  }

}
