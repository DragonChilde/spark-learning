package com.bigdata.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-05 19:42
 * */
object Spark01_RDD_Operator_Transform_Par {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    // TODO 算子 - map

    // 1. rdd的计算一个分区内的数据是一个一个执行逻辑
    //    只有前面一个数据全部的逻辑执行完毕后，才会执行下一个数据。
    //    分区内数据的执行是有序的。
    // 2. 不同分区数据计算是无序的。

    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4), 2)
    val mapRDD = rdd.map(
      num => {
        println(">>>>>>>>>" + num)
        num
      }
    )

    val mapRDD2 = mapRDD.map(
      num => {
        println("~~~~~~~~~~" + num)
        num
      }
    )

    /*
      >>>>>>>>>3
      >>>>>>>>>1
      ~~~~~~~~~~3
      ~~~~~~~~~~1
      >>>>>>>>>4
      ~~~~~~~~~~4
      >>>>>>>>>2
      ~~~~~~~~~~2
     */
    mapRDD2.collect()

    sparkContext.stop()

  }

}
