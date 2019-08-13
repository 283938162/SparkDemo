package com.spark.ml

import org.apache.spark.sql.{DataFrame, SparkSession}


object predict_rsrp {

  def load_data(): DataFrame = {
    val spark = SparkSession.builder().master("local").appName("fileRead").getOrCreate()
    import spark.implicits._
    val data: DataFrame = spark.read.format("com.databricks.spark.csv")
      .option("header", "true") //在csv第一行有属性"true"，没有就是"false"
      .option("inferSchema", true.toString) //这是自动推断属性列的数据类型
      .load("input/tt.csv")
    data
  }

  def main(args: Array[String]): Unit = {
    // 构造DataFrame
    val df = load_data()
    df.show()

    // Split the data into training and test sets (30% held out for testing)
    val splits = df.randomSplit(Array(0.7,0.3))
    val (trainData,testData) = (splits(0),splits(1))



  }
}
