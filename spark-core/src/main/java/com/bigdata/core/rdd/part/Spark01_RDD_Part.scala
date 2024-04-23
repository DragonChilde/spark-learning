package com.bigdata.core.rdd.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-23 08:18
 * */
object Spark01_RDD_Part {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(
      List(
        ("nba", "1111111"),
        ("cba", "2222222"),
        ("wnba", "3333333333333"),
        ("nba", "44444444")
      )
    )
    val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)

    partRDD.saveAsTextFile("output")
    sparkContext.stop()
  }


  /**
   * 自定义分区器
   * 1. 继承Partitioner
   * 2. 重写方法
   */
  class MyPartitioner extends Partitioner {
    override def numPartitions: Int = 3

    // 根据数据的key值返回数据所在的分区索引（从0开始）
    override def getPartition(key: Any): Int = {

      key match {
        case "nba" => 0
        case "wnba" => 1
        case _ => 2
      }
    }
  }

}
