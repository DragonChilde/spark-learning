package com.bigdata.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-04 10:37
 * */
object Spark_RDD_AllFile {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)
    // TODO 创建RDD
    // 从文件中创建RDD，将文件中的数据作为处理的数据源

    // textFile : 以行为单位来读取数据，读取的数据都是字符串
    // wholeTextFiles : 以文件为单位读取数据
    //    读取的结果表示为元组，第一个元素表示文件路径，第二个元素表示文件内容
    val rdd = sparkContext.wholeTextFiles("data")
    rdd.collect().foreach(println)
    // TODO 关闭环境
    sparkContext.stop()
  }

}
