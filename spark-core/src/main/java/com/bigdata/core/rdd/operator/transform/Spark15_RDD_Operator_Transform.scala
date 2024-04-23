package com.bigdata.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: reduceByKey
 * @author: JunWen
 * @create: 2024-04-13 08:00
 * */
object Spark15_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)
    // TODO 算子 - reduceByKey
    val rdd = sparkContext.makeRDD(
      List(
        ("a", 1), ("a", 2), ("a", 3), ("b", 4)
      )
    )

    // reduceByKey : 相同的key的数据进行value数据的聚合操作
    // scala语言中一般的聚合操作都是两两聚合，spark基于scala开发的，所以它的聚合也是两两聚合
    // 【1，2，3】
    // 【3，3】
    // 【6】
    // reduceByKey中如果key的数据只有一个，是不会参与运算的。
    val reduceRDD = rdd.reduceByKey(
      (x: Int, y: Int) => {
        println(s"x=${x},y=${y}")
        x + y
      }
    )
    reduceRDD.collect().foreach(println)

    sparkContext.stop()
  }
}
