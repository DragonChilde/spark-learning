package com.bigdata.core.test

import java.io.InputStream
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
    val server = new ServerSocket(9999)
    println("服务器启动,等待接收数据")

    // 等待客户端的连接
    val client: Socket = server.accept()
    val inputStream: InputStream = client.getInputStream

    val i: Int = inputStream.read()

    println("接收到客户羰发送的数据: " + i)
    inputStream.close()
    client.close()
    server.close()
  }
}
