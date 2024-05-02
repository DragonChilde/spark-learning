package com.bigdata.core.acc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-25 08:04
 * */
object Spark01_Acc {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4))

    // reduce : 分区内计算，分区间计算
    //val i: Int = rdd.reduce(_+_)
    //println(i)
    var sum = 0
    rdd.foreach(
      num => {
        sum += num
      }
    )

    println("sum = " + sum) //sum = 0

    sparkContext.stop()
  }

}
