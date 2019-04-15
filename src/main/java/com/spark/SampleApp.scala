package com.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SampleApp {
  def main(args: Array[String]) {
    val logFile = "/Users/anus/IdeaProjects/SparkDemo/input/wordcount_input.txt" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    //文件路径 和 最小分区数
    val logData = sc.textFile(logFile, 3).cache()

    //获取分区
    println("RDD分区数:"+logData.getNumPartitions)

//    val numAs = logData.filter(_.contains("a")).count()
//    val numBs = logData.filter(line => line.contains("b")).count()
//    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
