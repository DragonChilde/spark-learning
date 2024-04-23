package com.bigdata.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-04 09:54
 * */
object Spark_RDD_File_Partwo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)


    // TODO 创建RDD
    // TODO 数据分区的分配
    // 1. 数据以行为单位进行读取
    //    spark读取文件，采用的是hadoop的方式读取，所以一行一行读取，和字节数没有关系

    // 1.txt => 7 bytes => 7 / 2 => 3 这是偏移量
    // 7 / 3 => 2. 1 这是分区数,之前已有解释过

    // 2. 数据读取时以偏移量为单位,偏移量不会被重复读取
    /*
       1@@   => 012
       2@@   => 345
       3     => 6

     */
    // 3. 数据分区的偏移量范围的计算
    // 0 => [0, 3]  => 12
    // 1 => [3, 6]  => 3
    // 2 => [6, 7]  =>

    // 【1,2】，【3】，【】
    val rdd = sparkContext.textFile("data/1.txt", 2)

    rdd.saveAsTextFile("output")
    // TODO 关闭环境
    sparkContext.stop()
  }

}
