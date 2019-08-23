package com.spark.ml.官方学习案例

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * https://www.jianshu.com/p/9e90798d3cce
  *
  * 数据集路径: wget http://archive.ics.uci.edu/ml/machine-learning-databases/00275/Bike-Sharing-Dataset.zip
  *
  */
object BikeSharing {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val conf  = new SparkConf().setMaster("local").setAppName("bike sharing")
    var sc = new SparkContext(conf)

    val spark =SparkSession.builder().appName("").master("local").getOrCreate()

    val rawData = spark.read.format("csv").option("header", "true").load("data/Bike-Sharing-Dataset/hour.csv")
    rawData.show(5)

    //
    val rawDataRDD = rawData.rdd
    println(rawDataRDD.first())



  }
}
