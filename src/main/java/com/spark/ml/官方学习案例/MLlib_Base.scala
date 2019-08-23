package com.spark.ml.官方学习案例

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.{Matrices, Matrix}

object MLlib_Base {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)


  def main(args: Array[String]): Unit = {
    //    val dv: Vector = Vectors.dense(2.0, 0.0, 8.0)
    //    print(dv)
    //    val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))
    //    print(sm)

    //    val a = Array("zhan","li")
    //    a.map(_+"tttt")
    //    println(a)
    //    println(a.toList)
    //
    //    val b = Seq("zhan","li")
    //    b.map(_+"tttt")
    //    println(b)


    val conf = new SparkConf().setAppName("tianchi").setMaster("local")
    val sc = new SparkContext(conf)
    //    var rdd2 = sc.makeRDD(Seq("A", "B", "R", "D", "F"), 2)
    //    println(rdd2.zipWithIndex().collect.toMap)

//    val rawRDD = sc.textFile("data/sale.txt")
//    rawRDD.foreach(println)
//
//    val splitRDD = rawRDD.map(_.split(",")) // 这个RDD中都是Array对象
//    splitRDD.foreach(line => println(line.toList))
//
//    val numGoods = splitRDD.map(a => a(1)).count()
//    println(s"总售货数:${numGoods}")
//
//    val numUsers = splitRDD.map(a => a(0)).distinct.count()
//    println(s"用户数量:${numUsers}")
//
//    val totalGet = splitRDD.map(a => a(2).toDouble).collect().sum
//    println(s"总收入:${totalGet}")
//
//
//    val goodRDD = splitRDD.map(a => a(1)).map(word => (word, 1))
//    val reduceByKey = goodRDD.reduceByKey(_ + _).sortBy(_._2, false).collect()
//
//    println("产品" + "\t\t" + "销量")
//    for (term <- reduceByKey.toList) {
//      println(term._1 + ":" + term._2)
//    }

    val data = sc.textFile("data/sale.txt")
      .map(line => line.split(','))
      .map(purchaseRecord => (purchaseRecord(0), purchaseRecord(1), purchaseRecord(2)))


    val numPurchases: Long = data.count()

    //模式匹配 ???


    val uniqueUsers: Long =    data.map { case (user, product, price) => user }.distinct.count()
    val totalRevenue: Double = data.map { case (user, product, price) => price.toDouble }.sum()
    val productsByPopularity = data.map { case (user, product, price) => (product, 1) }
      .reduceByKey((x, y) => x + y).sortByKey(ascending=false).collect()
    val mostPopular = productsByPopularity(0)

    println("Total purchases: " + numPurchases)
    println("Unique users: " + uniqueUsers)
    println("Total revenue: " + totalRevenue)
    println("Most popular product: %s with %d purchases".format(mostPopular._1, mostPopular._2))

  }
}
