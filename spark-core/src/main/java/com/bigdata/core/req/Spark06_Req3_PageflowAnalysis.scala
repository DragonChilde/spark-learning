package com.bigdata.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: 页面单跳转换率统计
 * @author: JunWen
 * @create: 2024-04-29 16:51
 * */
object Spark06_Req3_PageflowAnalysis {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("PageFlowAnalysis")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    val actionRDD = sparkContext.textFile("data/user_visit_action.txt")

    val actionDataRDD = actionRDD.map(
      acttion => {
        val datas = acttion.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )
      }
    )

    actionDataRDD.cache()


    //  计算分母
    val pageIdToCountMap: Map[Long, Long] = actionDataRDD.map(
      action => {
        (action.page_id, 1L)
      }
    ).reduceByKey(_ + _).collect().toMap

    //  计算分子
    // 根据session进行分组
    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = actionDataRDD.groupBy(_.session_id)

    // 分组后，根据访问时间进行排序（升序）
    val mvRDD: RDD[(String, List[((Long, Long), Int)])] = sessionRDD.mapValues(
      iter => {
        val sortList: List[UserVisitAction] = iter.toList.sortBy(_.action_time)
        // 【1，2，3，4】
        // 【1，2】，【2，3】，【3，4】
        // 【1-2，2-3，3-4】
        // Sliding : 滑窗
        // 【1，2，3，4】
        // 【2，3，4】
        // zip : 拉链
        val flowIds: List[Long] = sortList.map(_.page_id)
        val pageflowIds: List[(Long, Long)] = flowIds.zip(flowIds.tail)

        pageflowIds.map(
          t => {
            (t, 1)
          }
        )
      }
    )

    // ((1,2),1)
    val flatRDD = mvRDD.map(_._2).flatMap(list => list)
    // ((1,2),1) => ((1,2),sum)
    val dataRDD = flatRDD.reduceByKey(_ + _)

    //  计算单跳转换率
    // 分子除以分母

    dataRDD.foreach {
      case ((pageid1, pageid2), sum) => {
        val lon = pageIdToCountMap.getOrElse(pageid1, 0L)
        println(s"页面${pageid1}跳转到页面${pageid2}单跳转换率为:" + (sum.toDouble / lon))
      }
    }

    // TODO 关闭连接
    sparkContext.stop()
  }


  //用户访问动作表
  case class UserVisitAction(
                              date: String, //用户点击行为的日期
                              user_id: Long, //用户的ID
                              session_id: String, //Session的ID
                              page_id: Long, //某个页面的ID
                              action_time: String, //动作的时间点
                              search_keyword: String, //用户搜索的关键词
                              click_category_id: Long, //某一个商品品类的ID
                              click_product_id: Long, //某一个商品的ID
                              order_category_ids: String, //一次订单中所有品类的ID集合
                              order_product_ids: String, //一次订单中所有商品的ID集合
                              pay_category_ids: String, //一次支付中所有品类的ID集合
                              pay_product_ids: String, //一次支付中所有商品的ID集合
                              city_id: Long
                            ) //城市 id
}
