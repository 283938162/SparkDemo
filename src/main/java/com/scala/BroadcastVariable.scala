package com.scala

import org.apache.spark.{SparkConf, SparkContext}

object BroadcastVariable {

  def broadcastvariable(): Unit = {
    val conf = new SparkConf()
      .setAppName("take")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val numberArray = Array(1,2,3,4,5,6,7,8,9,10)
    val numsRDD = sc.parallelize(numberArray)

    val factor = 3
    val factorBroadcast = sc.broadcast(factor)

    val count =numsRDD.map(num => num * factorBroadcast.value)
    for (num <- count){
      println(num)
    }
  }

//  注意： spark 只为accumulator 提供了累加的操作
  def accumulator(): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("accumulator")

    val sc = new SparkContext(conf)
    val sum = sc.accumulator(0)

    val numberArray = Array(1,2,3,4,5,6,7,8,9,10)
    val numsRDD = sc.parallelize(numberArray)

   numsRDD.foreach(num => sum += num)

    println(sum)


  }

  def main(args: Array[String]): Unit = {
//     broadcastvariable()
     accumulator()

  }
}
