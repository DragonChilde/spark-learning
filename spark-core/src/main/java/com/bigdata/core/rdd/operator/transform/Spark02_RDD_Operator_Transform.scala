package com.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-05 23:11
 * */
object Spark02_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    // TODO 算子 - mapPartitions
    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4), 2)

    // mapPartitions : 可以以分区为单位进行数据转换操作
    //                 但是会将整个分区的数据加载到内存进行引用
    //                 如果处理完的数据是不会被释放掉，存在对象的引用。
    //                 在内存较小，数据量较大的场合下，容易出现内存溢出。
    val mpRDD: RDD[Int] = rdd.mapPartitions(
      iter => {
        println(">>>>>>>>>")
        iter.map(_ * 2)
      }
    )
    mpRDD.collect().foreach(println)

    sparkContext.stop()
  }

}