package com.bigdata.core.acc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-25 09:59
 * */
object Spark02_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4))

    // 获取系统累加器
    // Spark默认就提供了简单数据聚合的累加器
    val sumAcc = sparkContext.longAccumulator("sum")

    /*    sparkContext.doubleAccumulator
        sparkContext.collectionAccumulator*/


    rdd.foreach(
      num => {
        // 使用累加器
        sumAcc.add(num)
      }
    )
    println(sumAcc.value) //10

    sparkContext.stop()
  }


}
