package com.scala

import org.apache.spark.{SparkConf, SparkContext}


object SparkTest {

  //  对文本内的数字，取出最大的前3个
  // 注释是第二种解法
  def getTop3(): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("getTop3")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("/Users/anus/IdeaProjects/SparkDemo/input/top3.txt")

    val mapLines = lines.map(x => (0, x))
    //    val mapLines = lines.map(x => (x.toInt,x))
    mapLines.foreach(num => println(num))

    val sortLines = mapLines.sortBy(_._2, false)
    //    val sortLines = mapLines.sortByKey(false)
    sortLines.foreach(num => println(num))

    val top3 = sortLines.take(3)

    for (num <- top3) {
      println(num)
    }

  }

  //  对每个班级内的学生成绩取出前3名（topN）

  /**
    *       * 1、通过map算子形成映射(class,score)
    *       * 2、通过groupByKey算子针对班级Key进行分组
    *       * 3、通过map算子对分组之后的数据取前3名，核心代码：val top3=m._2.toArray.sortWith(_>_).take(3)
    *       * 4、通过foreach算子遍历输出
    *       */

  def getScoreTop3(): Unit = {
    var conf = new SparkConf()
      .setMaster("local")
      .setAppName("topn")

    var sc = new SparkContext(conf)
    val lines = sc.textFile("/Users/anus/IdeaProjects/SparkDemo/input/score_top3.txt")
  }

  def main(args: Array[String]): Unit = {
    //    getTop3()
    getScoreTop3()
  }
}
