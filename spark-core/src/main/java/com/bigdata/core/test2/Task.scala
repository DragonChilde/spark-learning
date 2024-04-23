package com.bigdata.core.test2

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-02 21:48
 * */
class Task extends Serializable {

  val datas = List(1, 2, 3, 4)

  // val logic = (num:Int) => {num * 2}
  val logic: (Int) => Int = _ * 2

  // 计算
  def compute(): List[Int] = {
    datas.map(logic)
  }

}
