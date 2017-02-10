package com.ainspir.core.Stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hw201212 on 2017/2/9.
  */
object ScoketStreaming {

  def main(args: Array[String]) {
    //创建一个本地的StreamingContext，含2个工作线程
    val conf = new SparkConf().setMaster("local[2]").setAppName("ScoketStreaming")
    val sc = new StreamingContext(conf, Seconds(10)) //每隔10秒统计一次字符总数
    //创建珍一个DStream，连接master:9998
    sc.checkpoint(".")    //设置检查点，存储位置是当前目录，检查点具有容错机制
    val lines = sc.socketTextStream("localhost", 9998)
    val words = lines.flatMap(_.split(" "))
    words.foreachRDD(t => print(t))
    val wordCounts = words.map(x => 1)

    //======================================Window 相关操作开始=====================================================
    //reduceByWindow (v1: String, v2: Int),(v3: String, v4: Int)) => (v1+v3,v2+v4)
    //保存数据
//    val aa = wordCounts.countByValueAndWindow(Seconds(10), Seconds(10)).saveAsTextFiles("stream","txt")
    val aa = wordCounts.countByValueAndWindow(Seconds(10), Seconds(10))
    aa.print()

    //reduceByWindow (v1: String, v2: Int),(v3: String, v4: Int)) => (v1+v3,v2+v4)
    /*val aa = wordCounts.reduceByWindow((v1: (String,Int), v2: (String,Int)) => (v1._1+v2._1,v1._2 + v2._2),Seconds(10), Seconds(10))
    aa.print()*/

    //countByWindow
    /*val aa = wordCounts.countByWindow(Seconds(30), Seconds(10))
    aa.print()*/

    //reduceByKeyAndWindow window函数
    /*val searchWordCountsDStream = wordCounts.reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2, Seconds(30), Seconds(10))
    val finalDStream = searchWordCountsDStream.transform(searchWordCountsRDD => {
      val countSearchWordsRDD = searchWordCountsRDD.map(tuple => (tuple._2, tuple._1))
      val sortedCountSearchWordsRDD = countSearchWordsRDD.sortByKey(false)
      val sortedSearchWordCountsRDD = sortedCountSearchWordsRDD.map(tuple => (tuple._1, tuple._2))
      val top3SearchWordCounts = sortedSearchWordCountsRDD.take(3)

      for (tuple <- top3SearchWordCounts) {
        println("result : " + tuple)
      }

      searchWordCountsRDD
    })
    finalDStream.print()*/
    //======================================Window 相关操作结束=====================================================

    sc.start() //开始计算
    sc.awaitTermination() //通过手动终止计算，否则一直运行下去
  }

  /*def main(args: Array[String]) {
    //创建一个本地的StreamingContext，含2个工作线程
    val conf = new SparkConf().setMaster("local[2]").setAppName("ScoketStreaming")
    val sc = new StreamingContext(conf,Seconds(10))   //每隔10秒统计一次字符总数
    //创建珍一个DStream，连接master:9998
    val lines = sc.socketTextStream("localhost",9998)
    val words = lines.flatMap(_.split(" "))
    words.foreachRDD(t => print(t))
    val wordCounts = words.map(x => (x , 1)).reduceByKey(_ + _)
    wordCounts.print()
    sc.start()         //开始计算
    sc.awaitTermination()   //通过手动终止计算，否则一直运行下去
  }*/
}
