package com.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-13 08:15
 * */
object Spark16_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)
    // TODO 算子 - reduceByKey
    val rdd = sparkContext.makeRDD(
      List(
        ("a", 1), ("a", 2), ("a", 3), ("b", 4)
      )
    )

    // groupByKey : 将数据源中的数据，相同key的数据分在一个组中，形成一个对偶元组
    //              元组中的第一个元素就是key，
    //              元组中的第二个元素就是相同key的value的集合
    val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    groupRDD.collect().foreach(println)

    val groupRDD1: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)

    sparkContext.stop()
  }

}
