package com.bigdata.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-13 10:12
 * */
object Spark17_RDD_Operator_Transformthree {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)
    // TODO 算子 - aggregateByKey
    val rdd = sparkContext.makeRDD(
      List(
        ("a", 1), ("a", 2), ("b", 3),
        ("b", 4), ("b", 5), ("a", 6)
      ), 2
    )

    rdd.aggregateByKey(0)(_ + _, _ + _).collect().foreach(println)
    /*
    (b,12)
    (a,9)
     */
    // 如果聚合计算时，分区内和分区间计算规则相同，spark提供了简化的方法
    rdd.foldByKey(0)(_ + _).collect().foreach(println)
    /*
    (b,12)
    (a,9)
     */

    sparkContext.stop()
  }

}
