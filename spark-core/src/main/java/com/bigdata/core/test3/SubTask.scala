package com.bigdata.core.test3

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-02 22:03
 * */
class SubTask extends Serializable {

  var datas: List[Int] = _
  var logic: (Int) => Int = _

  // 计算
  def compute() = {
    datas.map(logic)
  }
}
