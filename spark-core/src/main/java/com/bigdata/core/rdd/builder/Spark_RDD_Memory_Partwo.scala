package com.bigdata.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-03 22:57
 * */
object Spark_RDD_Memory_Partwo {

  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    sparkConf.set("spark.default.parallelism", "5")
    val sparkContext = new SparkContext(sparkConf)

    // TODO 创建RDD
    // 【1，2】，【3，4】
    //val rdd = sparkContext.makeRDD(List(1,2,3,4), 2)
    // 【1】，【2】，【3，4】
    //val rdd = sparkContext.makeRDD(List(1,2,3,4), 3)
    // 【1】，【2,3】，【4,5】
    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4, 5), 3)
    // 将处理的数据保存成分区文件
    rdd.saveAsTextFile("output")
    // TODO 关闭环境
    sparkContext.stop()
  }
}
