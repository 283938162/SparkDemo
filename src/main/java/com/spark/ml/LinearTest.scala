package com.spark.ml

import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

object LinearTest {
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
    val training = spark.createDataFrame(Seq(
      (5.601801561245534, Vectors.sparse(10, Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), Array(0.6949189734965766, -0.32697929564739403, -0.15359663581829275, -0.8951865090520432, 0.2057889391931318, -0.6676656789571533, -0.03553655732400762, 0.14550349954571096, 0.034600542078191854, 0.4223352065067103))),
      (0.2577820163584905, Vectors.sparse(10, Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), Array(0.8386555657374337, -0.1270180511534269, 0.499812362510895, -0.22686625128130267, -0.6452430441812433, 0.18869982177936828, -0.5804648622673358, 0.651931743775642, -0.6555641246242951, 0.17485476357259122))),
      (1.5299675726687754, Vectors.sparse(10, Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), Array(-0.13079299081883855, 0.0983382230287082, 0.15347083875928424, 0.45507300685816965, 0.1921083467305864, 0.6361110540492223, 0.7675261182370992, -0.2543488202081907, 0.2927051050236915, 0.680182444769418))))).toDF("label", "features")
    training.show(false)


    //2 建立逻辑回归模型
    val lr = new LinearRegression()
      .setMaxIter(100)
      .setRegParam(0.1)
      .setElasticNetParam(0.5)

    //2 根据训练样本进行模型训练lib_svm_data.txt
    val lrModel = lr.fit(training)

    //2 打印模型信息
    println(s"Coefficients: ${lrModel.coefficients}, Intercept: ${lrModel.intercept}")

    //4 测试样本
    val test = spark.createDataFrame(Seq(
      (5.601801561245534, Vectors.sparse(10, Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), Array(0.6949189734965766, -0.32697929564739403, -0.15359663581829275, -0.8951865090520432, 0.2057889391931318, -0.6676656789571533, -0.03553655732400762, 0.14550349954571096, 0.034600542078191854, 0.4223352065067103))),
      (0.2577820163584905, Vectors.sparse(10, Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), Array(0.8386555657374337, -0.1270180511534269, 0.499812362510895, -0.22686625128130267, -0.6452430441812433, 0.18869982177936828, -0.5804648622673358, 0.651931743775642, -0.6555641246242951, 0.17485476357259122))),
      (1.5299675726687754, Vectors.sparse(10, Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), Array(-0.13079299081883855, 0.0983382230287082, 0.15347083875928424, 0.45507300685816965, 0.1921083467305864, 0.6361110540492223, 0.7675261182370992, -0.2543488202081907, 0.2927051050236915, 0.680182444769418))))).toDF("label", "features")
    test.show(false)

    //5 对模型进行测试
    val test_predict = lrModel.transform(test)
    test_predict
      .select("label", "prediction", "features")
      .show(false)



    //模型的保存与加载
    //    lrModel.save("ml_model/lrmodel")

//    val load_lrModel = LogisticRegressionModel.load("ml_model/lrmodel")
//    load_lrModel.transform(test).show(false)

  }

}
