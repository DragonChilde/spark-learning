package com.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-07 22:15
 * */
object Spark06_RDD_Operator_Transform_Test {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    // TODO 算子 - groupBy
    val rdd: RDD[String] = sparkContext.makeRDD(List("Hello", "hive", "hbase", "Hadoop"), 2)

    // 分组和分区没有必然的关系
    // 一个分区会有多个分组
    val groupRDD = rdd.groupBy(_.charAt(0))
    groupRDD.collect().foreach(println)
    sparkContext.stop()
  }

}
