package com.spark.ml.官方学习案例

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS, LogisticRegressionWithSGD}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * spark MLlib中的逻辑回归
  * https://www.jianshu.com/p/1002ba629549
  *
  */
object linear_models {

  def logistic_regression(): Unit = {
    //1.构建spark对象
    val conf = new SparkConf().setAppName("logistic_regression").setMaster("local")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    //读取样本数据,格式为LIBSVM format
    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
    // Split data into training(60%) and test(40%)
    // 训练街和测试集中分别包含了 label 和 features
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    println(training.count())
    training.collect().take(1).foreach(println)


    // Running training algoritthm to build the model
    // 未调优 使用默认参数   outcomes结果 效果
    val model = new LogisticRegressionWithLBFGS().setNumClasses(10).run(training)

    //    val model = LogisticRegressionWithSGD.train()  // 旧写法

    // compute raw scores on the test set
    // 旧方法 不兼容上面的model 报错
    //    val predictionAndLabels = test.map {
    //      case LabeledPoint(label, features) =>
    //        val prediction = model.predict(features)
    //        (prediction, label)
    //    }

    //    返回真实值与预测值组成的一个元组
    val labelsAndPredictions = test.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    labelsAndPredictions.foreach(println)


    val print_prediction = labelsAndPredictions.take(20)
    println("prediction" + "\t" + "label")
    for (i <- 0 to print_prediction.length - 1) {
      println(print_prediction(i)._1 + "\t" + print_prediction(i)._2)
    }

    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(labelsAndPredictions)
    val accuracy = metrics.accuracy
    println(s"Accuracy = $accuracy")

    // Save and load model
    checkDirExistBySpark(sc, "target/tmp/scalaLogisticRegressionWithLBFGSModel")
    model.save(sc, "target/tmp/scalaLogisticRegressionWithLBFGSModel")
    val sameModel = LogisticRegressionModel.load(sc, "target/tmp/scalaLogisticRegressionWithLBFGSModel")


  }

  def checkDirExistBySpark(sc: SparkContext, outpath: String) = {
    val hadoopconf = sc.hadoopConfiguration
    val hdfs = FileSystem.get(hadoopconf)
    if (hdfs.exists(new Path(outpath))) {
      try {
        //为防止误删，禁止递归删除
        // true 递归删除   flase 只删除指定路径
        hdfs.delete(new Path(outpath), true)
        print("输出目录存在，删除掉:%s".format(outpath))
      } catch {
        case _: Throwable => print("输出目录不存在，不用删除")
      }
    }
  }

//完全是spark2.x 使用ml包 api实现的机器学习 跟Python相似
  def logistic_regression_1(): Unit = {
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
    //多分类与上述类似，
  }

  def predict_cancer(): Unit = {
    import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
    import org.apache.spark.mllib.evaluation.MulticlassMetrics
    import org.apache.spark.mllib.linalg.Vectors
    import org.apache.spark.mllib.regression.LabeledPoint
    import org.apache.spark.mllib.util.MLUtils
    import org.apache.spark.{SparkConf, SparkContext}

    val conf = new SparkConf() //创建环境变量
      .setMaster("local")      //设置本地化处理
      .setAppName("LogisticRegression4")//设定名称
    val sc = new SparkContext(conf)

      val data = MLUtils.loadLibSVMFile(sc, "input/cancer.txt")	//读取数据文件,一定注意文本格式
      val splits = data.randomSplit(Array(0.7, 0.3), seed = 11L)	//对数据集切分
      val parsedData = splits(0)		//分割训练数据
      val parseTtest = splits(1)		//分割测试数据


      // 取对应索引的特征值
      println(parsedData.first().features(0))

     // 特征数量  parsedData RDD 数据集  first 取第一个 fretures 获取特征 size 获取长度
    println(parsedData.first().features.size)


    parsedData.foreach(println)
//      val model = LogisticRegressionWithSGD.train(parsedData,50)	//训练模型
//
//      val predictionAndLabels = parseTtest.map {//计算测试值
//        case LabeledPoint(label, features) =>	//计算测试值
//          val prediction = model.predict(features)//计算测试值
//          (prediction, label)			//存储测试和预测值
//      }
//
//      val metrics = new MulticlassMetrics(predictionAndLabels)//创建验证类
//      val precision = metrics.precision			//计算验证值
//      println("Precision = " + precision)	//打印验证值
//
//      val patient = Vectors.dense(Array(70,3,180.0,4,3))	//计算患者可能性
//      if(patient == 1) println("患者的胃癌有几率转移。")//做出判断
//      else println("患者的胃癌没有几率转移。")	//做出判断
      //Precision = 0.3333333333333333
      //患者的胃癌没有几率转移。
  }

  def main(args: Array[String]): Unit = {
//    logistic_regression()
//    logistic_regression_1()
      predict_cancer()
  }

}
