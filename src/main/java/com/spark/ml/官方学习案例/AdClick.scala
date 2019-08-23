package com.spark.ml.官方学习案例

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{Row, SparkSession}
//import org.apache.spark.ml.feature.FeatureHasher spark2.3 才有的api

/**
  * 基于逻辑回归的广告点击案例
  *
  * spark 介绍
  * spark ml对机器学习算法的api进行了标准化，使将多个算法合并到一个管道或工作流变得更容易。为了更清楚了解，从以下及几个方面展开说明。
  *
  * DataFrame:这个ML API使用Spark SQL的DataFrame作为ML数据集，它可以容纳各种数据类型。例如，DataFrame可能有不同的列存储文本、特征向量、真实标签和预测。
  *
  * Transformer: Transformer是一种可以将一个DataFrame转换成另一个DataFrame的算法。例如，ML模型是一个转换器，它将具有特性的DataFrame转换为具有预测的DataFrame。
  *
  * Estimator:估计器是一种算法，用于DataFrame转换。例如，学习算法是一种估计器，它训练一个DataFrame并生成一个模型。
  *
  * pipeline:管道将多个变压器和估计器链接在一起，以指定一个ML工作流。
  * ————————————————
  * 版权声明：本文为CSDN博主「Mr_哲」的原创文章，遵循CC 4.0 by-sa版权协议，转载请附上原文出处链接及本声明。
  * 原文链接：https://blog.csdn.net/baymax_007/article/details/82428984
  *
  * 包含24个字段：
  *
  * 1-id: ad identifier
  * 2-click: 0/1 for non-click/click
  * 3-hour: format is YYMMDDHH, so 14091123 means 23:00 on Sept. 11, 2014 UTC.
  * 4-C1 — anonymized categorical variable
  * 5-banner_pos
  * 6-site_id
  * 7-site_domain
  * 8-site_category
  * 9-app_id
  * 10-app_domain
  * 11-app_category
  * 12-device_id
  * 13-device_ip
  * 14-device_model
  * 15-device_type
  * 16-device_conn_type
  * 17~24—C14-C21 — anonymized categorical variables
  * 其中5到15列为分类特征，16~24列为数值型特征。将数据集分为训练集和测试集，比例为0.7：0.3。
  *
  */

object AdClick {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AdsCtrPredictionLR")
      .master("local[2]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    /**
      * id和click分别为广告的id和是否点击广告
      * site_id,site_domain,site_category,app_id,app_domain,app_category,device_id,device_ip,device_model为分类特征，需要OneHot编码
      * device_type,device_conn_type,C14,C15,C16,C17,C18,C19,C20,C21为数值特征，直接使用
      */
    val data = spark.read.csv("data/ad/train.csv").toDF(
      "id", "click", "hour", "C1", "banner_pos", "site_id", "site_domain",
      "site_category", "app_id", "app_domain", "app_category", "device_id", "device_ip",
      "device_model", "device_type", "device_conn_type", "C14", "C15", "C16", "C17", "C18",
      "C19", "C20", "C21")
    data.show(5, false)
    val splited = data.randomSplit(Array(0.7, 0.3), 2L)
    val catalog_features = Array("click", "site_id", "site_domain", "site_category", "app_id", "app_domain", "app_category", "device_id", "device_ip", "device_model")
    var train_index = splited(0)
    var test_index = splited(1)
    for (catalog_feature <- catalog_features) {
      val indexer = new StringIndexer()
        .setInputCol(catalog_feature)
        .setOutputCol(catalog_feature.concat("_index"))
      val train_index_model = indexer.fit(train_index)
      val train_indexed = train_index_model.transform(train_index)
      val test_indexed = indexer.fit(test_index).transform(test_index, train_index_model.extractParamMap())
      train_index = train_indexed
      test_index = test_indexed
    }
    println("字符串编码下标标签：")
    train_index.show(5, false)
    test_index.show(5, false)


    //    特征Hasher
    //    import org.apache.spark.ml.feature.FeatureHasher  这个api spark 2.3版本才更细

    //    val hasher = new FeatureHasher()
    //      .setInputCols("site_id_index", "site_domain_index", "site_category_index", "app_id_index", "app_domain_index", "app_category_index", "device_id_index", "device_ip_index", "device_model_index", "device_type", "device_conn_type", "C14", "C15", "C16", "C17", "C18", "C19", "C20", "C21")
    //      .setOutputCol("feature")
    //    println("特征Hasher编码：")
    //    val train_hs = hasher.transform(train_index)
    //    val test_hs = hasher.transform(test_index)
    /**
      * LR建模
      * setMaxIter设置最大迭代次数(默认100),具体迭代次数可能在不足最大迭代次数停止(见下一条)
      * setTol设置容错(默认1e-6),每次迭代会计算一个误差,误差值随着迭代次数增加而减小,当误差小于设置容错,则停止迭代
      * setRegParam设置正则化项系数(默认0),正则化主要用于防止过拟合现象,如果数据集较小,特征维数又多,易出现过拟合,考虑增大正则化系数
      * setElasticNetParam正则化范式比(默认0),正则化有两种方式:L1(Lasso)和L2(Ridge),L1用于特征的稀疏化,L2用于防止过拟合
      * setLabelCol设置标签列
      * setFeaturesCol设置特征列
      * setPredictionCol设置预测列
      * setThreshold设置二分类阈值
      */
//    val lr = new LogisticRegression()
//      .setMaxIter(10)
//      .setRegParam(0.3)
//      .setElasticNetParam(0)
//      .setFeaturesCol("feature")
//      .setLabelCol("click_index")
//      .setPredictionCol("click_predict")
//    val model_lr = lr.fit(train_hs)
//    println(s"每个特征对应系数: ${model_lr.coefficients} 截距: ${model_lr.intercept}")
//    val predictions = model_lr.transform(test_hs)
//    predictions.select("click_index", "click_predict", "probability").show(100, false)
//    val predictionRdd = predictions.select("click_predict", "click_index").rdd.map {
//      case Row(click_predict: Double, click_index: Double) => (click_predict, click_index)
//    }
//    val metrics = new MulticlassMetrics(predictionRdd)
//    val accuracy = metrics.accuracy
//    val weightedPrecision = metrics.weightedPrecision
//    val weightedRecall = metrics.weightedRecall
//    val f1 = metrics.weightedFMeasure
//    println(s"LR评估结果：\n分类正确率：${accuracy}\n加权正确率：${weightedPrecision}\n加权召回率：${weightedRecall}\nF1值：${f1}")
  }
}
