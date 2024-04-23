package com.bigdata.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: distinct
 * @author: JunWen
 * @create: 2024-04-10 11:17
 * */
object Spark09_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    // TODO 算子 - distinct
    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4, 1, 2, 3, 4))

    // map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)

    // (1, null),(2, null),(3, null),(4, null),(1, null),(2, null),(3, null),(4, null)
    // (1, null)(1, null)(1, null)
    // (null, null) => null
    // (1, null) => 1
    val rddDis = rdd.distinct()

    rddDis.collect().foreach(println)


    sparkContext.stop()
  }


}
