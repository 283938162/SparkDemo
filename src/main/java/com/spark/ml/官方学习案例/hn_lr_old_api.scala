package com.spark.ml.官方学习案例

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LinearRegressionModel


// 新旧不要写一起 导包冲突很恶心

/**
  * 湖南 根因定位 spark
  */
object hn_lr_old_api {
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR) //warn类信息不会显示，只显示error级别的

  def main(args: Array[String]): Unit = {
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

    data.show(5)
    import org.apache.spark.sql.functions._ // 一定要加这个不然很多报错

    //将dataframe中的数值类型转换为Double
    //    (1) 默认全部  val name = df.columns
    //    (2) 指定列   val name = "R1,R2,Y1"

    //    val name = data.columns
    //    data.select(name.map(name => col(name).cast(DoubleType)): _*).show()

    val name = "R1,R2,Y1"
    val df = data.select(name.split(",").map(name => col(name).cast(DoubleType)): _*)

    //    Map featues names to indices
    val featuresIndices = List("R1", "R2").map(df.columns.indexOf(_))
    featuresIndices.foreach(println)

    //Get index of target
    val labelIndex = data.columns.indexOf("Y1")

    //    val LabeledPointRdd = df.rdd.map{r => LabeledPoint(
    //      r.getDouble(labelIndex), // Get target value
    //      Vectors.dense(featuresIndices.map(r.getDouble(_)).toArray) // Map feature indices to values
    //    )}

    val LabeledPointRdd = df.rdd.map { r =>
      val label = r.getDouble(labelIndex) // Get target value
    val features = featuresIndices.map(r.getDouble(_)).toArray // Map feature indices to values
      LabeledPoint(label, Vectors.dense(features))
    }

    //样本划分训练集与测试集
    val Array(training, test) = LabeledPointRdd.randomSplit(Array(0.7, 0.3), seed = 11L)

    val numData = LabeledPointRdd.count
    val numTrainData = training.count
    val numTestData = test.count


    training.foreach(println)

    val numFeatures = training.first().features.size
    println("原始数据量：", numData)
    println("训练数据量：", numTrainData)
    println("测试数据量：", numTestData)

    println("特征值为度：", numFeatures)

    //新建逻辑回归模型并训练
    val numIterations = 10000
    val stepSize = 1
    val miniBatchFraction = 0.5
    val initialWeights = Vectors.dense(Array(0.0, 0.0))

    //    train参数说明：
    //
    //    input：样本数据，分类标签lable只能是1.0和0.0两种,feature为double类型
    //
    //    numIterations: 迭代次数，默认为100
    //
    //    stepSize: 迭代步长，默认为1.0
    //
    //    miniBatchFraction: 每次迭代参与计算的样本比例，默认为1.0
    //
    //    initialWeights:初始权重，默认为0向量

    //    估计被废弃了,无法被调用
    //    val model = LinearRegressionWithSGD.train(training, numIterations, stepSize, miniBatchFraction)

    var lr = new LinearRegressionWithSGD().setIntercept(true)
    lr.optimizer.setNumIterations(numIterations).setStepSize(stepSize).setMiniBatchFraction(miniBatchFraction)
    val model = lr.run(training)



    println(s"weights:${model.weights}, Intercept:${model.intercept}")


  }
}
