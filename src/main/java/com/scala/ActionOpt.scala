package com.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 1.reduce
  * 2.collect
  * 3.count
  * 4.take
  * 5.saveAsTextFile
  * 6.countByKey
  * 8.foreach  在远程的操作不像collect在本地
  *
  *
  * 很明显的区别是转换操作的结果还是RDD到RDD
  * 行动操作的结果是是从RDD到具体的数据类型
  */


object ActionOpt {

  def reduce(): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("reduce")

    val sc = new SparkContext(conf)
    val numberArray = Array(1,2,3,4,5,6,7,8,9,10)
    val numsRDD = sc.parallelize(numberArray)
    val sum =numsRDD.reduce(_+_)
    println(sum)
  }

  def collect(): Unit = {
    val conf = new SparkConf()
      .setAppName("collect")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val numberArray = Array(1,2,3,4,5,6,7,8,9,10)
    val numsRDD = sc.parallelize(numberArray)
    val numsDoubleRDD = numsRDD.map(_*2)
    val numsDouble = numsDoubleRDD.collect()

    println(numsDouble)
    for (num <- numsDouble){
      println(num)
    }


  }

  def count(): Unit = {
    val conf = new SparkConf()
      .setAppName("count")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val numberArray = Array(1,2,3,4,5,6,7,8,9,10)
    val numsRDD = sc.parallelize(numberArray)
    val count =numsRDD.count()
    println(count)
  }

  def take(): Unit = {
    val conf = new SparkConf()
      .setAppName("take")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val numberArray = Array(1,2,3,4,5,6,7,8,9,10)
    val numsRDD = sc.parallelize(numberArray)
    val count =numsRDD.take(3)
    for (num <- count){
      println(num)
    }
  }

  def saveAsTextfile(): Unit = {
    val conf = new SparkConf()
      .setAppName("take")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val numberArray = Array(1,2,3,4,5,6,7,8,9,10)
    val numsRDD = sc.parallelize(numberArray)
    val sf =numsRDD.saveAsTextFile("/Users/anus/IdeaProjects/SparkDemo/src/main/java/com/scala/out")
  }

  def countByKey(): Unit = {
    var conf = new SparkConf()
      .setMaster("local")
      .setAppName("countByKey")

    var sc = new SparkContext(conf)
    val scoreList = Array(Tuple2("class1",80),Tuple2("class2",20),Tuple2("class2",30))
    val scoreRDD = sc.parallelize(scoreList)
    var bykey = scoreRDD.countByKey()
    println(bykey)
    for (pair <- bykey){
      println(pair)
    }
  }

  def main(args: Array[String]): Unit = {
//     reduce()
//    collect()
//    count()
//    take()
//    saveAsTextfile()
    countByKey()
  }
}
