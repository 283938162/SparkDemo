package com.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 1.map 将集合中每个元素乘以2
  * 2.filter 过滤出集合中的偶数
  * 3.flatMap 将行拆分为单词
  * 4.groupByKey 将每个班级的成绩进行分组
  * 5.reduceByKey 统计每个班级的总分
  * 6.sortByKey 将学生分数进行排序
  * 7.join 打印每个学生的成绩
  * 8.cogroup 打印每个学生的成绩
  */

object TransformationOpt {
  def map(): Unit = {
    var conf = new SparkConf()
      .setAppName("map")
      .setMaster("local")
    var sc = new SparkContext(conf)

    val nums = Array(1, 2, 3, 4, 5)
    val numsRDD = sc.parallelize(nums)
    //    val mutiNumsRDD = numsRDD.map(num => num * 2)
    val mutiNumsRDD = numsRDD.map(_ * 2)
    //    mutiNumsRDD.foreach(println)

    mutiNumsRDD.foreach(num => println(num))
  }

  def filter(): Unit = {
    var conf = new SparkConf()
      .setAppName("filter")
      .setMaster("local")
    var sc = new SparkContext(conf)

    var numsRDD = sc.parallelize(List(1, 2, 4, 5, 6))
    //    val evenNumsRDD = numsRDD.filter(num => num % 2 ==0 )
    val evenNumsRDD = numsRDD.filter(_ % 2 == 0)
    evenNumsRDD.foreach(num => println(num + " is even!"))

  }

  def flatMap(): Unit = {
    var conf = new SparkConf()
      .setAppName("flatMap")
      .setMaster("local")
    val sc = new SparkContext(conf)
    var linesRDD = sc.parallelize(List("Hello Hi","Hello ho","Hello hi","Hello Hi"))
    var lineRDD = linesRDD.flatMap(line => line.split(" "))
    lineRDD.foreach(word => println(word))
  }

  def groupByKey(): Unit = {
    var conf = new SparkConf()
      .setMaster("local")
      .setAppName("groupByKey")

    var sc = new SparkContext(conf)
    val scoreList = Array(Tuple2("class1",80),Tuple2("class2",20),Tuple2("class2",30))
    val scoreRDD = sc.parallelize(scoreList)
    var groupedScores = scoreRDD.groupByKey()
    groupedScores.foreach(score => println(score))

  }

  def reduceByKey(): Unit = {
    var conf = new SparkConf()
      .setMaster("local")
      .setAppName("groupByKey")

    var sc = new SparkContext(conf)
    val scoreList = Array(Tuple2("class1",80),Tuple2("class2",20),Tuple2("class2",30))
    val scoreRDD = sc.parallelize(scoreList)
    var reducedScores = scoreRDD.reduceByKey(_ + _)
    reducedScores.foreach(classScore => println(classScore))
    reducedScores.foreach(classScore => println(classScore._1 + ":"+classScore._2))

  }

  def sortByKey(): Unit = {
    var conf = new SparkConf()
      .setMaster("local")
      .setAppName("groupByKey")

    var sc = new SparkContext(conf)
    val scoreList = Array(Tuple2("class1",80),Tuple2("class2",20),Tuple2("class2",30))
    val scoreRDD = sc.parallelize(scoreList)
    // 默认倒序  给个false参数升序
    var sortededScores = scoreRDD.sortByKey()
    sortededScores.foreach(classScore => println(classScore._1 + ":"+classScore._2))

  }

  def join(): Unit = {
    var conf = new SparkConf()
      .setMaster("local")
      .setAppName("join")

    var sc = new SparkContext(conf)
    val studentList = Array(
                        Tuple2(1,"zhangsan"),
                        Tuple2(2,"lisi"),
                        Tuple2(3,"wangwu"))
    val scoreList = Array(
      Tuple2(1,20),
      Tuple2(2,40),
      Tuple2(3,60))

    val studentRDD = sc.parallelize(studentList)
    val scoreRDD = sc.parallelize(scoreList)

    var studentScoreRDD = studentRDD.join(scoreRDD)

    studentScoreRDD.foreach(classScore => println(classScore._1 + ":"+classScore._2))

  }

  def main(args: Array[String]): Unit = {
//    map()
//    filter()
//    flatMap()
//    groupByKey()
//    reduceByKey()
//    sortByKey()

      join()
  }
}