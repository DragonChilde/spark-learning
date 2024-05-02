package com.bigdata.core.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-25 22:57
 * */
object Spark06_Bc {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val rdd1 = sparkContext.makeRDD(
      List(
        ("a", 1), ("b", 2), ("c", 3)
      )
    )

    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))
    // 封装广播变量
    val bc: Broadcast[mutable.Map[String, Int]] = sparkContext.broadcast(map)

    rdd1.map {
      case (w, c) => {
        // 方法广播变量
        val l = bc.value.getOrElse(w, 0)
        (w, (c, l))
      }
    }.collect().foreach(println)
    /*
    (a,(1,4))
    (b,(2,5))
    (c,(3,6))
     */

    sparkContext.stop()
  }

}
