package com.bigdata.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: foreach
 * @author: JunWen
 * @create: 2024-04-18 10:24
 * */
object Spark06_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)
    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4))

    // foreach 其实是Driver端内存集合的循环遍历方法
    rdd.collect().foreach(println)
    /*
    1
    2
    3
    4
     */
    println("******************")
    // foreach 其实是Executor端内存数据打印

    rdd.foreach(println)
    /*
    4
    3
    2
    1
     */

    // 算子 ： Operator（操作）
    //         RDD的方法和Scala集合对象的方法不一样
    //         集合对象的方法都是在同一个节点的内存中完成的。
    //         RDD的方法可以将计算逻辑发送到Executor端（分布式节点）执行
    //         为了区分不同的处理效果，所以将RDD的方法称之为算子。
    //        RDD的方法外部的操作都是在Driver端执行的，而方法内部的逻辑代码是在Executor端执行。

    sparkContext.stop()
  }

}
