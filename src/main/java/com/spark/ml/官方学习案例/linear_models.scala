package com.spark.ml.官方学习案例

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS, LogisticRegressionWithSGD}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

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

  def main(args: Array[String]): Unit = {
    logistic_regression()
  }

}
