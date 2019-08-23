package com.spark.ml.官方学习案例

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  *
  * 新旧api对比
  * https://dongkelun.com/2018/04/09/sparkMlLinearRegressionUsing/
  * (0) 特征上分两种 普通标签格式 另外一种是LIBSVM格式
  * (1) 旧版以RDD为基础 新版以DataFrame为基础
  * (2) 算法
  */
object logistic_regression_new_old_api_differ {


  def logistic_regression_old_api(): Unit = {
    import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
    import org.apache.spark.mllib.evaluation.MulticlassMetrics
    import org.apache.spark.mllib.linalg.Vectors
    import org.apache.spark.mllib.regression.LabeledPoint
    import org.apache.spark.mllib.util.MLUtils
    import org.apache.spark.{SparkConf, SparkContext}

    val conf = new SparkConf().setAppName("LogisticRegressionWithLBFGSExample").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // $example on$
    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(10)
      .run(training)

    // Compute raw scores on the test set.
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    val print_predict = predictionAndLabels.take(20)
    println("prediction" + "\t" + "label")

    for (i <- 0 to print_predict.length - 1) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }

    val patient = Vectors.dense(Array(70, 3, 180.0, 4, 3))
    val prediction = model.predict(patient)
    println(prediction)

    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val accuracy = metrics.accuracy
    println(s"Accuracy = $accuracy")

    // Save and load model
    //    model.save(sc, "target/tmp/scalaLogisticRegressionWithLBFGSModel")
    //    val sameModel = LogisticRegressionModel.load(sc,
    //      "target/tmp/scalaLogisticRegressionWithLBFGSModel")
    // $example off$

    sc.stop()
  }

  //  dataframe版本
  def logistic_regression_new_api(): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    import org.apache.spark.{SparkConf, SparkContext}
    import org.apache.spark.ml.classification.LogisticRegression
    import org.apache.spark.ml.linalg.{Vector, Vectors}
    import org.apache.spark.sql.{Row, SQLContext, SparkSession}
    import org.apache.spark.ml.param.ParamMap

    import org.apache.spark
    val spark = SparkSession.builder().master("local").appName("logistic_regression").getOrCreate()


    //准备训练集
    val training = spark.createDataFrame(Seq(
      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
      (1.0, Vectors.dense(0.0, 1.2, -0.5))
    )).toDF("label", "features")


    //准备测试集
    val test = spark.createDataFrame(Seq(
      (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
      (0.0, Vectors.dense(3.0, 2.0, -0.1)),
      (1.0, Vectors.dense(0.0, 2.2, -1.5))
    )).toDF("label", "features")

    //创建逻辑回归算法实例，并查看、设置相应参数
    val lr = new LogisticRegression()

    // 调用model.explainParams() 打印模型可调用的参数
//    println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")
    lr.setMaxIter(10).setRegParam(0.01)

    //训练学习得到model1,查看model1的参数
    //训练模型需要用到的参数,如果不掺入指定参数则为默认参数
    val model1 = lr.fit(training)
//    println("Model 1 was fit using parameters: " + model1.parent.extractParamMap)




//    不是调参,只是另外一种传参方式

    //用paraMap来设置参数集
    val paramMap = ParamMap(lr.maxIter -> 20).put(lr.maxIter, 30).put(lr.regParam -> 0.1, lr.threshold -> 0.55)
    //可以将两个paraMap结合起来
    val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")
    val paramMapCombined = paramMap ++ paramMap2
    print(paramMapCombined)
    //使用结合的paraMap训练学习得到model2
    val model2 = lr.fit(training, paramMapCombined)
    println("Model 2 was fit using parameters: " + model2.parent.extractParamMap)

    //使用测试集测试model2
    model2.transform(test)
      .select("features", "label", "myProbability", "prediction")
      .collect()
      .foreach {
        // 这里case Row的用法
        case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
          println(s"($features, $label) -> prob=$prob,prediction=$prediction")
      }

  }

  def logistic_regression_new_api_1(): Unit = {
    import org.apache.spark.ml.classification.LogisticRegression
    // Load training data
    val spark = SparkSession.builder().master("local").appName("logistc_regression").getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)
    import spark.implicits._

    val training = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
    // Fit the model
    val lrModel = lr.fit(training)
    // Print the coefficients and intercept for logistic regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    // We can also use the multinomial family for binary classification(二元分类)
    val mlr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFamily("multinomial")
    val mlrModel = mlr.fit(training)
    // Print the coefficients and intercepts for logistic regression with multinomial family
    println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")
    println(s"Multinomial intercepts: ${mlrModel.interceptVector}")
    //多分类与上述类似
  }

  def main(args: Array[String]): Unit = {
    //    logistic_regression_old_api()
    logistic_regression_new_api()
    //    logistic_regression_new_api_1()
  }
}
