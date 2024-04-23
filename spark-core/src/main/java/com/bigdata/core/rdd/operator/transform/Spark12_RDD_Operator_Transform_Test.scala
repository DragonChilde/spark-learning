package com.bigdata.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-10 21:58
 * */
object Spark12_RDD_Operator_Transform_Test {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    // TODO 算子 - sortBy
    val rdd = sparkContext.makeRDD(List(("1", 1), ("11", 2), ("2", 3)), 2)
    // sortBy方法可以根据指定的规则对数据源中的数据进行排序，默认为升序，第二个参数可以改变排序的方式
    // sortBy默认情况下，不会改变分区。但是中间存在shuffle操作
    val newRDD = rdd.sortBy(t => t._1.toInt, false)
    newRDD.collect().foreach(println)
    sparkContext.stop()
  }
}
