package com.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: 双Value类型
 * @author: JunWen
 * @create: 2024-04-11 08:40
 * */
object Spark13_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    // TODO 算子 - 双Value类型

    // 交集，并集和差集要求两个数据源数据类型保持一致
    // 拉链操作两个数据源的类型可以不一致

    val rdd1 = sparkContext.makeRDD(List(1, 2, 3, 4))
    val rdd2 = sparkContext.makeRDD(List(3, 4, 5, 6))

    // 交集 : 【3，4】
    val value: RDD[Int] = rdd1.intersection(rdd2)
    println(value.collect().mkString(","))

    // 并集 : 【1，2，3，4，3，4，5，6】
    val value2: RDD[Int] = rdd1.union(rdd2)
    println(value2.collect().mkString(","))

    // 差集 : 【1，2】
    val value3: RDD[Int] = rdd1.subtract(rdd2)
    println(value3.collect().mkString(","))

    // 拉链 : 【1-3，2-4，3-5，4-6】
    val value4: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(value4.collect().mkString(","))

    val rdd3 = sparkContext.makeRDD(List("3", "4", "5", "6"))
    //val value5 = rdd1.intersection(rdd7)
    //val value5 = rdd1.union(rdd3)
    //val value5 = rdd1.subtract(rdd3)
    val value6 = rdd1.zip(rdd3)
    println(value6.collect().mkString(","))

    sparkContext.stop()
  }

}
