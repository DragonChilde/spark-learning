package com.bigdata.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-04 09:54
 * */
object Spark_RDD_File_Par {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    // TODO 创建RDD
    // textFile可以将文件作为数据处理的数据源，默认也可以设定分区。
    //     minPartitions : 最小分区数量
    //     math.min(defaultParallelism, 2)
    //val rdd = sparkContext.textFile("data/1.txt")
    // 如果不想使用默认的分区数量，可以通过第二个参数指定分区数
    // Spark读取文件，底层其实使用的就是Hadoop的读取方式
    // 分区数量的计算方式：
    //    totalSize = 7
    //    goalSize =  7 / 2 = 3（byte）
    //    7 / 3 = 2...1 (1.1) + 1 = 3(分区)

    // 注意:这里指的最小分区,生成的分区数未必为2
    val rdd = sparkContext.textFile("data/1.txt", 2)

    rdd.saveAsTextFile("output")
    // TODO 关闭环境
    sparkContext.stop()
  }

}
