package com.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: partitionBy
 * @author: JunWen
 * @create: 2024-04-11 09:30
 * */
object Spark14_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)
    // TODO 算子 - partitionBy
    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4), 2)
    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))
    // RDD => PairRDDFunctions
    // 隐式转换（二次编译）
    // partitionBy根据指定的分区规则对数据进行重分区
    val newRDD = mapRDD.partitionBy(new HashPartitioner(2))
    //newRDD.partitionBy(new HashPartitioner(2))
    newRDD.saveAsTextFile("output")

    sparkContext.stop()
  }

}
