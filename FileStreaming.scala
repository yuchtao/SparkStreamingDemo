package com.ainspir.core.Stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hw201212 on 2017/2/9.
  */
object FileStreaming {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("FileStreaming")
    val sc = new StreamingContext(conf,Seconds(5))
    val lines = sc.textFileStream("file:\\\\\\E:\\test")
    val words = lines.flatMap(_.split(","))
    words.count().print();
    val wordCounts = words.map(x => (x , 1)).reduceByKey(_ + _)
    wordCounts.count().print()
    sc.start()
    sc.awaitTermination()
  }
}
