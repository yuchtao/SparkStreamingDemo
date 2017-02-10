package com.ainspir.core.Stream

import java.io.{BufferedReader, InputStreamReader}
import java.net.{ConnectException, Socket}
import java.nio.charset.StandardCharsets

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hw201212 on 2017/2/9.
  */
object CustomReceiver {
  def main(args: Array[String]) {
    //创建一个本地的StreamingContext，含2个工作线程
    val conf = new SparkConf().setMaster("local[3]").setAppName("ScoketStreaming")
    val sc = new StreamingContext(conf,Seconds(10))   //每隔10秒统计一次字符总数
    //创建珍一个DStream，连接master:9998
    val lines = sc.receiverStream(new CustomReceiver("localhost",9998))
    val words = lines.flatMap(_.split(" "))
    words.foreachRDD(t => print(t))
    val wordCounts = words.map(x => (x , 1)).reduceByKey(_ + _)
    wordCounts.print()
    sc.start()         //开始计算
    sc.awaitTermination()   //通过手动终止计算，否则一直运行下去
  }
}

class CustomReceiver(host:String, port: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging{
  override def onStart(){
    new Thread("Socket Receiver"){
      override def run() { receive() }
    }.start()
  }

  override def onStop(){

  }

  private def receive(){
    var socket:Socket = null;
    var userInput: String = null;
    try{
      logInfo("Connecting to " + host + ":" + port)
      socket = new Socket(host, port)
      logInfo("Connected to " + host + ":" + port)
      val reader = new BufferedReader(
        new InputStreamReader(socket.getInputStream()))
      userInput = reader.readLine()
      println("外面 userInput:========== "+userInput)
      while(!isStopped && userInput != null) {
        store(userInput)
        userInput = reader.readLine()
        println("里面 userInput:========== "+userInput)
      }
      reader.close()
      socket.close()
      logInfo("Stopped receiving")
      restart("Trying to connect again")
    } catch {
      case e: ConnectException =>
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        restart("Error receiving data", t)
    }
  }
}
