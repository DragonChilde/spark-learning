package com.bigdata.core.test2

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
    val client = new Socket("localhost", 8888)

    val outputStream: OutputStream = client.getOutputStream

    val objectOutputStream: ObjectOutputStream = new ObjectOutputStream(outputStream)

    val task = new Task()
    objectOutputStream.writeObject(task)

    objectOutputStream.flush()
    objectOutputStream.close()
    client.close()
    println("客户端发送成功!")
  }
}
