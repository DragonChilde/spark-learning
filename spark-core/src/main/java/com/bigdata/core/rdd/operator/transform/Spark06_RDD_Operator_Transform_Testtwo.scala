package com.bigdata.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-07 22:28
 * */
object Spark06_RDD_Operator_Transform_Testtwo {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    // TODO 算子 - groupBy
    val rdd = sparkContext.textFile("data/apache.log")
    val timeRDD = rdd.map(
      line => {
        val datas = line.split(" ")
        val time = datas(3)
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val date: Date = sdf.parse(time)
        val dateFormat = new SimpleDateFormat("HH")
        val hour: String = dateFormat.format(date)
        (hour, 1)
      }

    ).groupBy(_._1)


    timeRDD.map {
      case (hour, iter) => {

        (hour, iter.size)
      }
    }.collect().foreach(println)
    sparkContext.stop()
  }

}
