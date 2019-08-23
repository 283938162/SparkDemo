package com.spark.ml.官方学习案例

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression

/**
  * 利用线性回归模型对数据进行回归预测
  */
object predict_murder {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    //设置环境
    val conf = new SparkConf().setAppName("tianchi").setMaster("local")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder().master("local").appName("test").getOrCreate()

    //准备训练集合
    val raw_data = sc.textFile("input/LR_data.txt")
    val map_data = raw_data.map { x =>
      val split_list = x.split(",")
      (split_list(0).toDouble, split_list(1).toDouble, split_list(2).toDouble, split_list(3).toDouble, split_list(4).toDouble, split_list(5).toDouble, split_list(6).toDouble, split_list(7).toDouble)
    }

    map_data.foreach(println)

    // 此时的map_data 只是普通的rdd
    //    (3615.0,3624.0,2.1,69.05,15.1,41.3,20.0,50708.0)
    //    (365.0,6315.0,1.5,69.31,11.3,66.7,152.0,566432.0)
    //    (2212.0,4530.0,1.8,70.55,7.8,58.1,15.0,113417.0)

    // Creates a `DataFrame` from an RDD of Product (e.g. case classes, tuples)
    // 竟然可以直接转换成DF，看注释是支持tuples的
    val df = spark.createDataFrame(map_data)

    val data = df.toDF("Population", "Income", "Illiteracy", "LifeExp", "Murder", "HSGrad", "Frost", "Area")
    val colArray = Array("Population", "Income", "Illiteracy", "LifeExp", "HSGrad", "Frost", "Area")


    // 这种手段去组合特征集 真是优秀
    val assembler = new VectorAssembler().setInputCols(colArray).setOutputCol("features")
    val vecDF: DataFrame = assembler.transform(data)

    vecDF.show(5, false)


    //准备预测集合
    val raw_data_predict = sc.textFile("input/LR_data_for_predict.txt")
    val map_data_for_predict = raw_data_predict.map { x =>
      val split_list = x.split(",")
      (split_list(0).toDouble, split_list(1).toDouble, split_list(2).toDouble, split_list(3).toDouble, split_list(4).toDouble, split_list(5).toDouble, split_list(6).toDouble, split_list(7).toDouble)
    }
    val df_for_predict = spark.createDataFrame(map_data_for_predict)
    val data_for_predict = df_for_predict.toDF("Population", "Income", "Illiteracy", "LifeExp", "Murder", "HSGrad", "Frost", "Area")
    val colArray_for_predict = Array("Population", "Income", "Illiteracy", "LifeExp", "HSGrad", "Frost", "Area")
    val assembler_for_predict = new VectorAssembler().setInputCols(colArray_for_predict).setOutputCol("features")
    val vecDF_for_predict: DataFrame = assembler_for_predict.transform(data_for_predict)

    // 建立模型，预测谋杀率Murder
    // 设置线性回归参数
    val lr1 = new LinearRegression()
    val lr2 = lr1.setFeaturesCol("features").setLabelCol("Murder").setFitIntercept(true)
    // RegParam：正则化
    val lr3 = lr2.setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    val lr = lr3

    // 将训练集合代入模型进行训练
    val lrModel = lr.fit(vecDF)

    // 输出模型全部参数
    lrModel.extractParamMap()
    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // 模型进行评价
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")


    val predictions: DataFrame = lrModel.transform(vecDF_for_predict)
    //    val predictions = lrModel.transform(vecDF)
    println("输出预测结果")
    val predict_result: DataFrame = predictions.selectExpr("features", "Murder", "round(prediction,1) as prediction")
    predict_result.foreach(println(_))
    sc.stop()
  }
}
