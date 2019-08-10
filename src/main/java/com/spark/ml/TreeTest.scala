package com.spark.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.{DecisionTreeClassifier, GBTClassifier, LogisticRegressionModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession


/**
  * 机器学习各项指标
  * https://blog.csdn.net/weixin_42111770/article/details/81015809
  */

object TreeTest {
  def main(args: Array[String]): Unit = {
    // 0.构建 Spark 对象
    val spark = SparkSession
      .builder()
      .master("local") // 本地测试，否则报错 A master URL must be set in your configuration at org.apache.spark.SparkContext.
      .appName("test")
      //      .enableHiveSupport()
      .getOrCreate() // 有就获取无则创建


    spark.sparkContext.setCheckpointDir("checkpoint") //设置文件读取、存储的目录，HDFS最佳
    import spark.implicits._

    //1 训练样本准备
    //1 训练样本准备
    val data = spark.read.format("libsvm").load("ml_dataSets/lib_svm_data.txt")

    data.show(false)

    //2 标签进行索引编号
    val labelIndexer = new StringIndexer().
      setInputCol("label").
      setOutputCol("indexedLabel").
      fit(data)

    labelIndexer.labels.foreach(println)


    // 对离散特征进行标记索引，以用来确定哪些特征是离散特征
    // 如果一个特征的值超过4个以上，该特征视为连续特征，否则将会标记得离散特征并进行索引编号
    val featureIndexer = new VectorIndexer().
      setInputCol("features").
      setOutputCol("indexedFeatures").
      setMaxCategories(4).
      fit(data)

    //    featureIndexer.categoryMaps.foreach(println)


    //3 样本划分
    //https://blog.csdn.net/SA14023053/article/details/51993727
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
    //4 训练决策树模型
    val dt = new DecisionTreeClassifier().
      setLabelCol("indexedLabel").
      setFeaturesCol("indexedFeatures")

    //4 训练随机森林模型
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(10)

    //4 训练GBDT模型
    val gbt = new GBTClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setMaxIter(10)

    //5 将索引的标签转回原始标签
    val labelConverter = new IndexToString().
      setInputCol("prediction").
      setOutputCol("predictedLabel").
      setLabels(labelIndexer.labels)

    //6 构建Pipeline
    val pipeline1 = new Pipeline().
      setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

    val pipeline2 = new Pipeline().
      setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    val pipeline3 = new Pipeline().
      setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))

    //7 Pipeline开始训练
    val model1 = pipeline1.fit(trainingData)

    val model2 = pipeline2.fit(trainingData)

    val model3 = pipeline3.fit(trainingData)

    //8 模型测试
    val predictions = model1.transform(testData)
    predictions.show(5,false)

    //8 测试结果
    predictions.select("predictedLabel", "label", "features").show(5,false)

    //9 分类指标
    // 正确率
    val evaluator1 = new MulticlassClassificationEvaluator().
      setLabelCol("indexedLabel").
      setPredictionCol("prediction").
      setMetricName("accuracy")
    val accuracy = evaluator1.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))


  }

}
