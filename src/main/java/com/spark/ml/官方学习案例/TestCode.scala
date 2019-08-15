package com.spark.ml.官方学习案例

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object TestCode {

  //屏蔽不必要的日志显示在终端上
  //Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR) //warn类信息不会显示，只显示error级别的
  Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)

  def test1(): Unit = {
    val spark = SparkSession.builder().master("local").appName("hn_lr").getOrCreate()


    /**
      * 参数说明：
      * delimiter 分隔符，默认为逗号,
      * nullValue 指定一个字符串代表 null 值
      * quote 引号字符，默认为双引号"
      * header  true 第一行不作为数据内容，作为标题
      * inferSchema 自动推测字段类型
      */
    val data = spark.read.format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .option("quote", "'")
      .option("nullValue", "\\N")
      .option("inferSchema", "true")
      .load("input/train.csv")


    val rdd = data.rdd.take(10)
    //    rdd.foreach(println)
    import org.apache.spark.ml.linalg.{Vector, Vectors}

    val a = Array(1.0, 2.0, 3.0)
    println(a)
    println(a(0))

    val dv: Vector = Vectors.dense(1, 2, 3)
    println(dv)


    val aa = "1,2"
    val bb = aa.split(",")
    println(bb.map(_.toDouble))
    bb.map(_.toDouble).foreach(println)


  }


//  Spark ML里的核心API已经由基于RDD换成了基于DataFrame
  def test2(): Unit = {
    import org.apache.spark.ml.feature.LabeledPoint
    import org.apache.spark.ml.linalg.Vectors

    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)

    val spark =SparkSession.builder().master("local").appName("test").getOrCreate()
    // 读取数据并分割每个样本点的属性值 形成一个Array[String]类型的RDD
    val rdd = sc.textFile("input/t.txx").map(_.split(","))
    // 将rdd转换成LabeledPoint类型的RDD
    val LabeledPointRdd = rdd.map(x=>LabeledPoint(0,Vectors.dense(x.map(_.toDouble))))
    // 转成DataFrame并只取"features"列
    val data = spark.createDataFrame(LabeledPointRdd).select("features")
    data.show()


  }

  def main(args: Array[String]) {
    //    test1()
    test2()

  }
}
