package com.bigdata.core.test

import java.io.OutputStream
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
    val client = new Socket("localhost", 9999)

    val outputStream: OutputStream = client.getOutputStream
    outputStream.write(2)
    outputStream.flush()
    outputStream.close()
    client.close()
  }
}
