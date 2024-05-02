package com.bigdata.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: 统计用户页面操作
 * @author: JunWen
 * @create: 2024-04-26 16:01
 * */
object Spark01_Req1_HotCategoryTop10Analysis {

  def main(args: Array[String]): Unit = {
    // TODO : Top10热门品类
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sparkContext = new SparkContext(sparkConf)


    // 1. 读取原始日志数据
    val actionRDD = sparkContext.textFile("data/user_visit_action.txt")

    // 2. 统计品类的点击数量：（品类ID，点击数量）
    val clickActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(6) != "-1"
      }
    )

    val clickCountRDD = clickActionRDD.map(
      action => {
        val datas = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)

    // 3. 统计品类的下单数量：（品类ID，下单数量）
    val orderActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(8) != "null"
      }
    )
    // orderid => 1,2,3
    // 【(1,1)，(2,1)，(3,1)】
    val orderCountRDD = orderActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(8)
        val cids = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)


    // 4. 统计品类的支付数量：（品类ID，支付数量）
    val payActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(10) != "null"
      }
    )

    // orderid => 1,2,3
    // 【(1,1)，(2,1)，(3,1)】
    val payCountRDD = payActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(10)
        val cids = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)

    // 5. 将品类进行排序，并且取前10名
    //    点击数量排序，下单数量排序，支付数量排序
    //    元组排序：先比较第一个，再比较第二个，再比较第三个，依此类推
    //    ( 品类ID, ( 点击数量, 下单数量, 支付数量 ) )
    //
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCountRDD.cogroup(orderCountRDD, payCountRDD)
    val analysisRDD = cogroupRDD.mapValues {

      case (clickIter, orderIter, payIter) => {
        var clickCnt = 0
        val clickIterator = clickIter.iterator
        if (clickIterator.hasNext) {
          clickCnt = clickIterator.next()
        }

        var orderCnt = 0
        val orderIterator = orderIter.iterator
        if (orderIterator.hasNext) {
          orderCnt = orderIterator.next()
        }
        var payCnt = 0
        val payIterator = payIter.iterator
        if (payIterator.hasNext) {}
        payCnt = payIterator.next()

        (clickCnt, orderCnt, payCnt)
      }
    }

    val resultRDD = analysisRDD.sortBy(_._2, false).take(10)


    // 6. 将结果采集到控制台打印出来
    resultRDD.foreach(println)

    sparkContext.stop()
  }

}
