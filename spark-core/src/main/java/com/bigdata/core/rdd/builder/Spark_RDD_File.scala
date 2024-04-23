package com.bigdata.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-04 09:54
 * */
object Spark_RDD_File {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)
    // TODO 创建RDD
    // 从文件中创建RDD，将文件中的数据作为处理的数据源
    // path路径默认以当前环境的根路径为基准。可以写绝对路径，也可以写相对路径
    //sparkContext.textFile("E:\\Learning\\spark-learning\\data\\1.txt")
    //val rdd: RDD[String] = sparkContext.textFile("data/1.txt")
    // path路径可以是文件的具体路径，也可以目录名称
    val rdd: RDD[String] = sparkContext.textFile("data")
    // path路径还可以使用通配符 *
    //val rdd: RDD[String] = sparkContext.textFile("data/1*.txt")
    // path还可以是分布式存储系统路径：HDFS
    //val rdd = sparkContext.textFile("hdfs://linux1:8020/test.txt")

    rdd.collect().foreach(println)
    // TODO 关闭环境
    sparkContext.stop()
  }

}
