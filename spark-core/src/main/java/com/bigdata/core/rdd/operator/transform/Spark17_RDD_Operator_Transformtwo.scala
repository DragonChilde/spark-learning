package com.bigdata.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-13 09:01
 * */
object Spark17_RDD_Operator_Transformtwo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)
    // TODO 算子 - aggregateByKey
    val rdd = sparkContext.makeRDD(
      List(
        ("a", 1), ("a", 2), ("b", 3),
        ("b", 4), ("b", 5), ("a", 6)
      ), 2
    )

    // aggregateByKey存在函数柯里化，有两个参数列表
    // 第一个参数列表,需要传递一个参数，表示为初始值
    //       主要用于当碰见第一个key的时候，和value进行分区内计算
    // 第二个参数列表需要传递2个参数
    //      第一个参数表示分区内计算规则
    //      第二个参数表示分区间计算规则

    // math.min(x, y)
    // math.max(x, y)
    rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    ).collect().foreach(println)
    /*
   (b,8)
    (a,8)
     */

    rdd.aggregateByKey(5)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    ).collect().foreach(println)
    /*
    (b,10)
    (a,11)
     */

    rdd.aggregateByKey(0)(
      (x, y) => x + y,
      (x, y) => x + y
    ).collect().foreach(println)
    /*
    (b,12)
    (a,9)
     */

    rdd.aggregateByKey(0)(_ + _, _ + _).collect().foreach(println)
  /*
  (b,12)
  (a,9)
   */
    sparkContext.stop()
  }


}
