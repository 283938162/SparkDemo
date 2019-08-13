package com.spark.ml.官方学习案例
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.log4j.{Level, Logger}

/**
  * 参考: https://spark.apache.org/docs/latest/mllib-ensembles.html
  *
  * 集成学习 Ensembles
  * 1.RandomForest
  * 2.gradient-Boosted Trees (FBTS)
  *
  * An ensemble method is a learning algorithm which creates a model composed of a set of other base models.
  * spark.mllib supports two major ensemble algorithms: GradientBoostedTrees and RandomForest. Both use decision trees as their base models.
  *
  */


object Ensembles {

  def rf(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("RandomForest").set("spark.hadoop.validateOutputSpecs","true")
    val sc = new SparkContext(conf)
    //屏蔽日志
    Logger.getRootLogger.setLevel(Level.WARN)

    // Load and parse the data file.
    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 3 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 4
    val maxBins = 32

    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)



    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println(s"Test Error = $testErr")
    println(s"Learned classification forest model:\n ${model.toDebugString}")

    checkDirExistByHadoop(sc,"target/tmp/myRandomForestClassificationModel")

    // Save and load model
    model.save(sc, "target/tmp/myRandomForestClassificationModel")
    val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModel")
  }


  // 通过spark自带的hadoopconf方式删除
  def checkDirExistBySpark(sc:SparkContext,outpath:String) = {
    val hadoopconf = sc.hadoopConfiguration
    val hdfs = FileSystem.get(hadoopconf)
    if(hdfs.exists(new Path(outpath))){
      try{
        //为防止误删，禁止递归删除
        // true 递归删除   flase 只删除指定路径
        hdfs.delete(new Path(outpath),true)
        print("输出目录存在，删除掉:%s".format(outpath))
      } catch {
        case _:Throwable => print("输出目录不存在，不用删除")
      }
    }
  }

  // 通过Hadoop方式删除已存在的文件目录
  def checkDirExistByHadoop(sc:SparkContext,outpath:String) = {

    val hdfs = FileSystem.get(new URI(outpath),new Configuration())
    if(hdfs.exists(new Path(outpath))){
      try{
        hdfs.delete(new Path(outpath),true)
        print("输出目录存在，删除掉:%s".format(outpath))
      } catch {
        case _:Throwable => print("输出目录不存在，不用删除")
      }
    }
  }




  def main(args: Array[String]): Unit = {
     rf()
  }
}
