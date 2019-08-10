package com.scala

import org.apache.spark.{SparkConf, SparkContext}


// 按照文件中的第一列排序
// 如果第一列相同，则按照第二列排序
object SecondSort {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
      .setAppName("SecondSort")
      .setMaster("local")

    val sc = new SparkContext(conf)

    var lines = sc.textFile("/Users/anus/IdeaProjects/SparkDemo/input/secondsort.txt")
    lines.foreach(line => println(line))


    //    关键是怎么做到恰如其当的映射
    val pairs = lines.map{line => (
      new SecondSortKey(line.split(" ")(0).toInt,line.split(" ")(1).toInt),line
    )}

    pairs.foreach(pair => println(pair))

    val sortedPairs = pairs.sortByKey()
    sortedPairs.foreach(pair => println(pair))


    val sortedLines = sortedPairs.map(sortedPair => sortedPair._2)
    sortedLines.foreach(sortedLine => println(sortedLine))









  }
}
