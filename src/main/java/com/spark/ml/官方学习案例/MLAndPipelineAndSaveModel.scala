package com.spark.ml.官方学习案例

import javax.xml.transform.stream.StreamResult
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{PCA, StandardScaler, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.dmg.pmml.PMML
import org.jpmml.model.JAXBUtil
//import org.jpmml.sparkml.PMMLBuilder

import java.nio.charset.StandardCharsets
import java.nio.file.StandardOpenOption._
import java.nio.file.{Files, Paths}
import javax.xml.transform.stream.StreamResult

import org.dmg.pmml.Application
import org.jpmml.model.JAXBUtil
import org.jpmml.sparkml.ConverterUtil

//import acme.ACMEModel


/**
  * 逻辑回归 Pipeline 完整版
  * 本地版 保存模型为PMML格式  目前ml包不支持 直接保存PMML文件    mllib包可以直接 model.toPMML()
  */
object MLAndPipelineAndSaveModel {

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

  //  def saveToLocalFile(pmml: PMML, path: String): Unit = {
  //    JAXBUtil.marshalPMML(pmml, new StreamResult(path))
  //  }

  //屏蔽日志
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  //主函数 程序入口
  def main(args: Array[String]): Unit = {

    // 创建SparkSession
    val spark = SparkSession
      .builder()
      .appName("MLAndPipeline")
      .master("local")
      .getOrCreate()

    //spark 和 SparkContext 可以转换  sc = spark.sparkContext

    //加载训练数据生成 DataFame
    val data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    //    println(data.first().get(1))
    //    println(s"样本总数:${data.count()},每条样本包含${data.first().size}条特征")

    //归一化 指定归一化对象 后者列  fit(dataset: Dataset[_]):StandardScalerModel
    //归一化后新增一列存储归一化后的数据 列名为 outputCol 有自己指定
    val scaler = new StandardScaler()
      .setInputCol("features") //对特征列进行归一化
      .setOutputCol("scaledFeatures")
      .setWithMean(true) // 设置均差
      .setWithStd(true) //标准差
      .fit(data)


    //    Transformer 变换器：变换器是一种可以将一个DataFrame转换为另一个DataFrame的算法
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

    //    output.show(5)

    //输出csv格式的标签和主成分,便于可视化
    val ass = output.select(output("assemble").cast("string"))
    ass.write.mode("overwrite").csv("output.csv")

    //将经过主成分分析的数据,按比例划分为训练数据和测试数据
    val Array(trainingData, testData) = pcaResult.randomSplit(Array(0.7, 0.3), seed = 11L)


    //Estimator 估计器：估计器是一种可以适合DataFrame以产生变换器的算法。 例如，学习算法是在DataFrame上训练并产生模型的估计器。
    // 创建SVC分类器(Estimator)
    val lsvc = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.1)


    //创建pipeline,将上述步骤连接起来
    //Pipeline 管道：管道将  多个变换器和 估计器链接在一起，以指定ML工作流。
    val pipeline = new Pipeline()
      .setStages(Array(scaler, pca, lsvc))

    //使用串联好的模型在训练集上训练, //注意spark懒加载特性
    val model = pipeline.fit(trainingData)

    // 在测试集上测试  这些写法确实比RDD的写法灵活很多
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
    checkDirExistBySpark(spark.sparkContext, "out")

    //保存模型
    //    model.save("out")

    //保存成PMML格式
    //    val schema = trainingData.schema
    //    val pmml = new PMMLBuilder(schema, model).build()
    //    saveToLocalFile(pmml, "pmml_out")


    //    val training = ACMEData.readData()
    //    val pipeline = ACMEModel.buildModel()

    //    val pmml = ConverterUtil.toPMML(trainingData.schema,model)
    //    pmml.getHeader.setApplication(new Application("ACME Occupancy Detection"))

    //    val modelPath = Paths.get("src", "main", "resources")
    //    if (!Files.exists(modelPath)) {
    //      Files.createDirectory(modelPath)
    //    }
    //    val pmmlFile = modelPath.resolve("model.pmml")
    //    val writer = Files.newBufferedWriter(pmmlFile, StandardCharsets.UTF_8, WRITE, CREATE, TRUNCATE_EXISTING)
    //    try {
    //      JAXBUtil.marshalPMML(pmml, new StreamResult(writer))
    //    } finally {
    //      writer.close()
    //    }


    spark.stop()


  }

}
