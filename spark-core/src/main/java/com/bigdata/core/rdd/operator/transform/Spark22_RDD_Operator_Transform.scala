package com.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: cogroup
 * @author: JunWen
 * @create: 2024-04-17 07:58
 * */
object Spark22_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    // TODO 算子 - cogroup
    val rdd1 = sparkContext.makeRDD(
      List(
        ("a", 1), ("b", 2)
      )
    )
    val rdd2 = sparkContext.makeRDD(
      List(
        ("a", 4), ("b", 5), ("c", 6), ("c", 7)
      )
    )

    // cogroup : connect + group (分组，连接)
    val cgRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
    cgRDD.collect().foreach(println)
    /*
    (a,(Seq(1),Seq(4)))
    (b,(Seq(2),Seq(5)))
    (c,(Seq(),Seq(6, 7)))
     */

    sparkContext.stop()
  }


}
