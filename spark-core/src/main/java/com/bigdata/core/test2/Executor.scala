package com.bigdata.core.test2

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

/**
 * @program: spark-learning
 * @description: ${description}
 * @author: JunWen
 * @create: 2024-04-02 14:49
 * */
object Executor {

  def main(args: Array[String]): Unit = {
    // 启动服务器,接收数据
    val server = new ServerSocket(8888)
    println("服务器启动,等待接收数据")

    // 等待客户端的连接
    val client: Socket = server.accept()
    val inputStream: InputStream = client.getInputStream


    val objectInputStream = new ObjectInputStream(inputStream)
    val task: Task = objectInputStream.readObject().asInstanceOf[Task]

    val value: List[Int] = task.compute()

    println("计算节点计算的结果为: " + value)
    objectInputStream.close()
    client.close()
    server.close()
  }
}
