package com.spark.ml.官方学习案例


import org.apache.hadoop.hdfs.util.Diff.ListType
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, DoubleType, StringType}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors


/**
  * 湖南 根因定位 spark
  */
object hn_lr {
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR) //warn类信息不会显示，只显示error级别的
  //  Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)

  def new_ml_api(): Unit = {
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
    import org.apache.spark.sql.functions._
    import spark.implicits._ // 一定要加这个不然很多报错

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

    val LabeledPointRdd = df.rdd.map(r => LabeledPoint(
      r.getDouble(labelIndex), // Get target value
      Vectors.dense(featuresIndices.map(r.getDouble(_)).toArray) // Map feature indices to values
    ))

    // RDD[LabeledPoint] 转 DataFrame
    val dataF = spark.createDataFrame(LabeledPointRdd).select("label", "features")
    dataF.show()


    // 以上阶段都是数据准备阶段

    //划分训练集与测试集
    val Array(trainingData, testData) = dataF.randomSplit(Array(0.7, 0.3), seed = 11L)

    trainingData.cache()
    testData.cache()
    val numData = data.count
    val numTrainData = trainingData.count
    val numTestData = testData.count

    val numFeatures = trainingData.first().size
    println("原始数据量：", numData)
    println("训练数据量：", numTrainData)
    println("测试数据量：", numTestData)

    println("特征值为度：", numFeatures)


    // 创建LR分类器(Estimator)
    val lr = new LogisticRegression()
      .setMaxIter(10000)
      .setRegParam(0.1)

    //训练分类器,生成模型(Trandformer)
    val lrModel = lr.fit(trainingData)





    // 打印参数 这个是输入模型的参数
    println("Model 1 was fit using parameters: " + lrModel.parent.extractParamMap)

    //怎么打印特征权重?和 模型偏执
    println(s"Coefficients:${lrModel.coefficients}, Intercept:${lrModel.intercept}")


    //用训练好的模型验证测试数据  transform的作用
    val res = lrModel.transform(testData)
    res.printSchema()
    //    root
    //    |-- label: double (nullable = false)
    //    |-- features: vector (nullable = true)
    //    |-- rawPrediction: vector (nullable = true)
    //    |-- probability: vector (nullable = true)
    //    |-- prediction: double (nullable = true)

    // 打印预测值与实际值
    res.select("prediction", "label").show()


    //计算精度
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(res.select("prediction", "label"))

    //输出格式 ${}
    println(s"Accuracy = ${accuracy}")
  }

  def main(args: Array[String]): Unit = {
      new_ml_api()
  }

}
