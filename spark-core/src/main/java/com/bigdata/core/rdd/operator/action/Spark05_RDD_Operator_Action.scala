package com.bigdata.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-18 10:20
 * */
object Spark05_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)
    val rdd = sparkContext.makeRDD(
      List(("a", 1), ("a", 2), ("a", 3))
    )

    // TODO - 行动算子
    // 保存成 Text 文件
    rdd.saveAsTextFile("output")
    // 序列化成对象保存到文件
    rdd.saveAsObjectFile("output1")
    // 保存成 Sequencefile 文件
    // saveAsSequenceFile方法要求数据的格式必须为K-V类型
    rdd.saveAsSequenceFile("output2")

    sparkContext.stop()
  }
}
