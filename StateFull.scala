package com.ainspir.core.Stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{State, StateSpec, Seconds, StreamingContext}

/**
  * Created by hw201212 on 2017/2/9.
  */
object StateFull {
  //updateStateByKey
  def main(args: Array[String]) {
    //定义状态更新函数
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      println("==============================",values.mkString(","),state.getOrElse(-1))
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      println("==============================",values.mkString(","),state.getOrElse(-1))
      println("currentCount:"+currentCount,"previousCount:"+previousCount)
      Some(currentCount + previousCount)
    }
    val conf = new SparkConf().setMaster("local[2]").setAppName("stateFull")
    val sc = new StreamingContext(conf, Seconds(10))
    sc.checkpoint(".")    //设置检查点，存储位置是当前目录，检查点具有容错机制
    val lines = sc.socketTextStream("localhost",9998)
    val words = lines.flatMap(_.split(" "))
    val wordDstream: DStream[(String, Int)] = words.map(x => (x, 1))

    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      println("==============================word:"+word,"one:"+one.getOrElse(-1),"state:"+state.getOption.getOrElse(-1),"sum:"+sum)
      val output = (word, sum)
      state.update(sum)
      output
    }
    val stateDstream = wordDstream.mapWithState(StateSpec.function(mappingFunc))
    stateDstream.print()

    sc.start()
    sc.awaitTermination()
  }

  /*def main(args: Array[String]) {
    //定义状态更新函数
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      println("==============================",values.mkString(","),state.getOrElse(-1))
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      println("==============================",values.mkString(","),state.getOrElse(-1))
      println("currentCount:"+currentCount,"previousCount:"+previousCount)
      Some(currentCount + previousCount)
    }
    val conf = new SparkConf().setMaster("local[2]").setAppName("stateFull")
    val sc = new StreamingContext(conf, Seconds(10))
    sc.checkpoint(".")    //设置检查点，存储位置是当前目录，检查点具有容错机制
    val lines = sc.socketTextStream("localhost",9998)
    val words = lines.flatMap(_.split(" "))
    val wordDstream: DStream[(String, Int)] = words.map(x => (x, 1))
    val stateDstream = wordDstream.updateStateByKey[Int](updateFunc)
    stateDstream.print()
    sc.start()
    sc.awaitTermination()
  }*/
}
