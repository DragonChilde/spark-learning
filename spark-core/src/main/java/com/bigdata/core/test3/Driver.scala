package com.bigdata.core.test3

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-02 14:48
 * */
object Driver {

  def main(args: Array[String]): Unit = {
    // 启动服务器,接收数据
    val clientOne = new Socket("localhost", 8888)
    val clientTwo = new Socket("localhost", 7777)
    val task = new Task()

    val outputStreamOne: OutputStream = clientOne.getOutputStream
    val objectOutputStreamOne: ObjectOutputStream = new ObjectOutputStream(outputStreamOne)

    val subTaskOne = new SubTask()
    subTaskOne.logic = task.logic
    subTaskOne.datas = task.datas.take(2)

    objectOutputStreamOne.writeObject(subTaskOne)
    objectOutputStreamOne.flush()
    objectOutputStreamOne.close()
    clientOne.close()


    val outputStreamTwo: OutputStream = clientTwo.getOutputStream
    val objectOutputStreamTwo: ObjectOutputStream = new ObjectOutputStream(outputStreamTwo)

    val subTaskTwo = new SubTask()
    subTaskTwo.logic = task.logic
    subTaskTwo.datas = task.datas.takeRight(2)

    objectOutputStreamTwo.writeObject(subTaskTwo)
    objectOutputStreamTwo.flush()
    objectOutputStreamTwo.close()
    clientTwo.close()

    println("客户端发送成功!")
  }
}
