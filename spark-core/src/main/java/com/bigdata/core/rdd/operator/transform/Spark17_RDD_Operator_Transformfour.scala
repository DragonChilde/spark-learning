package com.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-13 11:11
 * */
object Spark17_RDD_Operator_Transformfour {
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
    // aggregateByKey最终的返回数据结果应该和初始值的类型保持一致
    //val aggRDD: RDD[(String, String)] = rdd.aggregateByKey("")(_ + _, _ + _)
    //aggRDD.collect.foreach(println)

    // 获取相同key的数据的平均值 => (a, 3),(b, 4)
    val newRDD = rdd.aggregateByKey((0, 0))(
      (t, v) => {
        //0 + 1,0 + 1
        //1 + 2, 1 + 1
        //3 + 6 , 2 + 1
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )

    val resultRDD: RDD[(String, Int)] = newRDD.mapValues {
      case (num, cnt) => {
        num / cnt
      }
    }
    resultRDD.collect().foreach(println)
    /*
    (b,12)
    (a,9)
     */

    sparkContext.stop()
  }


}
