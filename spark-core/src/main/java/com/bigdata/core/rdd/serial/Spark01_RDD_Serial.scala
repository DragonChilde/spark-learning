package com.bigdata.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-18 16:14
 * */
object Spark01_RDD_Serial {
  def main(args: Array[String]): Unit = {
    //1.创建 SparkConf 并设置 App 名称
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    //2.创建 SparkContext，该对象是提交 Spark App 的入口
    val sparkContext = new SparkContext(sparConf)
    //3.创建一个 RDD
    val rdd = sparkContext.makeRDD(Array("hello world", "hello spark", "hive", "flink"))
    //3.1 创建一个 Search 对象
    val search = new Search("h")
    //3.2 函数传递，打印：ERROR Task not serializable
    //search.getMatch1(rdd).collect().foreach(println)

    search.getMatch2(rdd).collect().foreach(println)
    //4.关闭连接
    sparkContext.stop()
  }

  // 查询对象
  // 类的构造参数其实是类的属性, 构造参数需要进行闭包检测，其实就等同于类进行闭包检测
  class Search(query: String) {
    def isMatch(s: String): Boolean = {
      s.contains(this.query)
    }

    // 函数序列化案例
    def getMatch1(rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)
    }

    // 属性序列化案例
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      val s = query
      rdd.filter(x => x.contains(s))
    }

  }

}
