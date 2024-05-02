package com.bigdata.core.acc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-25 10:41
 * */
object Spark03_Acc {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4))

    // 获取系统累加器
    // Spark默认就提供了简单数据聚合的累加器
    val sumAcc = sparkContext.longAccumulator("sum")
    /*    sparkContext.doubleAccumulator
        sparkContext.collectionAccumulator*/

    val mapRDD = rdd.map(
      num => {
        // 使用累加器
        sumAcc.add(num)
        num
      }
    )
    // 获取累加器的值
    // 少加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行
    // 多加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行
    // 一般情况下，累加器会放置在行动算子进行操作
/*    mapRDD.collect()
    mapRDD.collect()*/

    println(sumAcc.value) //20

    sparkContext.stop()
  }

}
