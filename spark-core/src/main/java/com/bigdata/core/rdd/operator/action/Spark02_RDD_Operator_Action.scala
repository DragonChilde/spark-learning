package com.bigdata.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-18 08:08
 * */
object Spark02_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)
    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4))

    // TODO - 行动算子
    val i: Int = rdd.reduce(_ + _)
    println(i) //10

    // collect : 方法会将不同分区的数据按照分区顺序采集到Driver端内存中，形成数组
    val arr: Array[Int] = rdd.collect()
    println(arr.mkString(",")) //1,2,3,4

    // count : 数据源中数据的个数
    val cnt = rdd.count()
    println(cnt) //4

    val first = rdd.first()
    println(first) //  1

    // take : 获取N个数据
    val ints: Array[Int] = rdd.take(3)
    println(ints.mkString(",")) //1,2,3

    // takeOrdered : 数据排序后，取N个数据
    val rdd2 = sparkContext.makeRDD(List(4, 3, 2, 1))
    val ints1: Array[Int] = rdd2.takeOrdered(3)
    println(ints1.mkString(",")) //1,2,3

    sparkContext.stop()
  }
}
