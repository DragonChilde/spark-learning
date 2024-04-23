package com.bigdata.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-06 19:45
 * */
object Spark04_RDD_Operator_Transform_Test {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    // TODO 算子 - flatMap
    val rdd = sparkContext.makeRDD(List(List(1, 2), 3, List(4, 5)))

    val flatRDD = rdd.flatMap(

      data => {
        data match {
          // 匹配集合类型直接返回集合
          case list: List[_] => list
          // 不是集合类型就包装返回集合类型
          case dat => List(dat)
        }
      }
    )

    flatRDD.collect().foreach(println)
    sparkContext.stop()
  }

}
