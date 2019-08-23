package com.spark.ml.官方学习案例

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LinearSVC, LogisticRegression}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{PCA, StandardScaler, VectorAssembler}
import org.apache.spark.sql.SparkSession


/**
  * 逻辑回归 Pipeline 完整版
  * 服务器 集群部署版
  *
  * 单机版到集群版:(1) 去掉local (2) 路径参数传入
  *
  *
  * spark2-submit \
  * --class com.spark.ml.官方学习案例.MLAndPipelineServer \
  * --master yarn \
  * --driver-memory 2G \
  * --executor-memory 2G \
  * /home/liushengjie/ml/SparkDemo-1.0-SNAPSHOT.jar \
  * /input/sample_libsvm_data.txt \
  * /input/models
  *
  */
object MLAndPipelineServer {

  //屏蔽日志
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def checkDirExistBySpark(sc: SparkContext, outpath: String) = {
    val hadoopconf = sc.hadoopConfiguration

    val hdfs = FileSystem.get(hadoopconf)
    if (hdfs.exists(new Path(outpath))) {
      try {
        //为防止误删，禁止递归删除
        // true 递归删除   flase 只删除指定路径
        hdfs.delete(new Path(outpath), true)
        println("输出目录存在，删除掉:%s".format(outpath))
      } catch {
        case _: Throwable => print("输出目录不存在，不用删除")
      }
    }
  }

  //主函数 程序入口 传入参数 训练文本路径,模型保存路径
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: <file>")
      System.exit(1)
    }


    val conf = new SparkConf().setAppName("MLAndPipeline-sc")
    val sc = new SparkContext(conf)

    // 创建SparkSession
    val spark = SparkSession
      .builder()
      .appName("MLAndPipeline-spark")
      .getOrCreate()


    //加载训练数据生成 DataFame
    //    hdfs://192.168.5.221:9000/input/sample_libsvm_data.txt
    val data = spark.read.format("libsvm").load(args(0))

    //    println(data.first().get(1))
    //    println(s"样本总数:${data.count()},每条样本包含${data.first().size}条特征")

    //归一化 fit(dataset: Dataset[_]):StandardScalerModel
    val scaler = new StandardScaler()
      .setInputCol("features") //对特征列进行归一化
      .setOutputCol("scaledFeatures")
      .setWithMean(true) // 设置均差
      .setWithStd(true) //标准差
      .fit(data)


    //    val scalaedData = scaler.transform(data).select("label","scaledFeatures").toDF("label","features")
    val scalaedData = scaler.transform(data).select("label", "scaledFeatures").withColumnRenamed("scaledFeatures", "features")
    //    scalaedData.show(5)

    //创建PCA模型,生成Transformer
    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(5)
      .fit(scalaedData)


    // transform 数据 生成主成分特征
    val pcaResult = pca.transform(scalaedData).select("label", "pcaFeatures").toDF("label", "features")
    //    pcaResult.show(5)

    //将标签与主成分合并成一列
    val assembler = new VectorAssembler()
      .setInputCols(Array("label", "features"))
      .setOutputCol("assemble")
    val output = assembler.transform(pcaResult)

    output.show(5)

    //输出csv格式的标签和主成分,便于可视化
    val ass = output.select(output("assemble").cast("string"))
    ass.write.mode("overwrite").csv("output.csv")

    //将经过主成分分析的数据,按比例划分为训练数据和测试数据
    val Array(trainingData, testData) = pcaResult.randomSplit(Array(0.7, 0.3), seed = 11L)

    // 创建SVC分类器(Estimator)
    val lsvc = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.1)


    //创建pipeline,将上述步骤连接起来
    val pipeline = new Pipeline()
      .setStages(Array(scaler, pca, lsvc))

    //使用串联好的模型在训练集上训练, //注意spark懒加载特性
    val model = pipeline.fit(trainingData)

    // 在测试集上测试
    val predictions = model.transform(testData).select("prediction", "label")

    predictions.show(20)

    // 计算精度
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)

    println(s"Accuracy = ${accuracy}")


    //保存之前检测路径是否存在
    checkDirExistBySpark(sc, args(1))
    //保存模型
    model.save(args(1))

    spark.stop()

  }

}
