package com.bigdata.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-04 09:54
 * */
object Spark_RDD_File_Parthree {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)


    // TODO 创建RDD
    // 14byte / 2 = 7byte
    // 14 / 7 = 2(分区)

    /*
    1234567@@  => 012345678
    89@@       => 9101112
    0          => 13

    [0, 7]   => 1234567
    [7, 14]  => 890

     */

    // 如果数据源为多个文件，那么计算分区时以文件为单位进行分区
    val rdd = sparkContext.textFile("data/word.txt", 2)


    rdd.saveAsTextFile("output")
    // TODO 关闭环境
    sparkContext.stop()
  }

}
