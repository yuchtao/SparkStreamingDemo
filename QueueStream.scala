package com.ainspir.core.Stream

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hw201212 on 2017/2/9.
  */
object QueueStream {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[3]").setAppName("queueStream")
    //每1秒对数据进行处理
    val ssc = new StreamingContext(conf,Seconds(1))
    //创建一个能够push到QueueInputDStream的RDDs队列
    val rddQueue = new scala.collection.mutable.SynchronizedQueue[RDD[Int]]()
    //基于一个RDD队列创建一个输入源
    val inputStream = ssc.queueStream(rddQueue)
    val mappedStream = inputStream.map(x => (x % 10,1))
    val reduceStream = mappedStream.reduceByKey(_ + _)
    reduceStream.print
    ssc.start()
    for(i <- 1 to 3){
      rddQueue += ssc.sparkContext.makeRDD(1 to 100, 2)   //创建RDD，并分配两个核数
      Thread.sleep(1000)
    }
    ssc.stop()
  }
}
