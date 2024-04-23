package com.bigdata.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-18 15:45
 * */
object Spark07_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    //val rdd = sparkContext.makeRDD(List(1, 2, 3, 4))
    val rdd = sparkContext.makeRDD(List())
    val user = new User()

    // SparkException: Task not serializable
    //Caused by: java.io.NotSerializableException: com.bigdata.core.operator.action.Spark07_RDD_Operator_Action$User
    // RDD算子中传递的函数是会包含闭包操作，那么就会进行检测功能
    // 闭包检测
    rdd.foreach(
      num => {
        println("age = " + user.age + num)
      }
    )
    sparkContext.stop()
  }

    class User {
      var age: Int = 35
    }


  /*
    class User extends Serializable {
      var age: Int = 35
    }
  */

  // 样例类在编译时，会自动混入序列化特质（实现可序列化接口）
/*
  case class User() {
    var age: Int = 35
  }
*/

}
