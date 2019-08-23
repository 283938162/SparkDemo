package com.spark.ml.官方学习案例

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{ClassificationModel, LogisticRegressionWithSGD}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.{SimpleUpdater, SquaredL2Updater, Updater}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.impurity.{Gini, Impurity}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
//import org.apache.spark.ml.feature.FeatureHasher spark2.3 才有的api

/**
  * 基于逻辑回归的广告点击案例
  * boilerplate 文件模板  范例
  *
  * https://www.jianshu.com/p/83d4cb4a5697?utm_campaign=maleskine&utm_content=note&utm_medium=seo_notes&utm_source=recommendation
  * https://blog.csdn.net/lovebyz/article/details/51791572
  * https://www.jianshu.com/p/357a83ee5f24
  */

object AdClick1 {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    //    val spark = SparkSession.builder()
    //      .appName("AdsCtrPredictionLR")
    //      .master("local[2]")
    //      .config("spark.some.config.option", "some-value")
    //      .getOrCreate()

    //    val data = spark.read.format("csv")
    //      .option("delimiter", "\t")
    //      .option("header", "true")
    //      .option("quote", "'")
    //      .option("nullValue", "\\N")
    //      .option("inferSchema", "true")
    //      .load("data/ad/train.tsv")
    //
    //    data.show(5)


    //    val dataLP = data.map(
    //      r =>
    //        val trimmed = r
    //    )

    //    import spark.implicits._
    //    val dataLP = data.map(_.toString().replaceAll("\"",""))
    //    dataLP.show(5,false)


    val conf = new SparkConf().setAppName("ad click").setMaster("local")
    val sc = new SparkContext(conf)

    val rawData = sc.textFile("data/ad/train.tsv")
    //    rawData.foreach(println)


    //    (1) 方法一 位置很重要 如果split之后 便失效了
    val header = rawData.first()
    val lines = rawData.filter(_ != header)
    //    lines.foreach(println)

    //    (2) 方法二  跳过前n行
    //    val n = 1
    //    rawData.zipWithIndex().filter(_._2>=n)
    //    rawData.foreach(println)


    val records = lines.map(_.split("\t"))
    //    val header = records.first()
    //
    //    lines.foreach(println)


    val data = records.map {
      r =>
        val trimmed = r.map(_.replaceAll("\"", ""))

        //取单个值 直接index 如果是切片,则需要slice接入  slice(start,end)
        //if else 写法
        val label = trimmed(trimmed.size - 1).toInt
        val features = trimmed.slice(4, trimmed.size - 1).map(x => if (x == "?") 0.0 else x.toDouble)
        //label存分类结果，features存特征，将其转换为LabeledPoint类型，此类型主要用于监督学习。
        //这种写法需要导入mllib 而不是 ml包
        LabeledPoint(label, Vectors.dense(features))
    }


    //    data.foreach(println)

    data.cache()
    println(s"样本数:${data.count()},每条样本有:${data.first().features.size}条特征")


    //贝叶斯模型需要特征值非负，否则碰到负的特征值程序会抛出错误。
    //需要对特征值中的某个值做下判断 将负值赋值为0
    val nbData = records.map {
      r =>
        val trimmed = r.map(_.replaceAll("\"", ""))

        //取单个值 直接index 如果是切片,则需要slice接入  slice(start,end)
        //if else 写法
        val label = trimmed(trimmed.size - 1).toInt
        val features = trimmed.slice(4, trimmed.size - 1).map(x => if (x == "?") 0.0 else x.toDouble).map(x => if (x > 0) x else 0.0)
        //label存分类结果，features存特征，将其转换为LabeledPoint类型，此类型主要用于监督学习。
        //这种写法需要导入mllib 而不是 ml包
        LabeledPoint(label, Vectors.dense(features))
    }


    import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
    import org.apache.spark.mllib.classification.SVMWithSGD
    import org.apache.spark.mllib.classification.NaiveBayes

    import org.apache.spark.mllib.tree.DecisionTree
    import org.apache.spark.mllib.tree.configuration.Algo
    import org.apache.spark.mllib.tree.impurity.Entropy
    val numIteration = 10
    val maxTreeDepth = 5


    //训练模型
    val lrModel = LogisticRegressionWithSGD.train(data, numIteration)

    val svmModel = SVMWithSGD.train(data, numIteration)

    val nbModel = NaiveBayes.train(nbData)

    val dtModel = DecisionTree.train(data, Algo.Classification, Entropy, maxTreeDepth)


    //使用模型
    //    val lrPrediction = lrModel.predict(data.map(_.features))
    //    val lrPredictionAndLable = lrPrediction.zip(testData.map(_.label))
    //
    //    val top20LrPredictionAndLable = lrPredictionAndLable.take(20)
    //    println("Prediction" + "\t" + "True")
    //    for (i <- 0 to top20LrPredictionAndLable.length - 1) {
    //      println(top20LrPredictionAndLable(i)._1 + "\t\t\t" + top20LrPredictionAndLable(i)._2)
    //    }

    // Get evaluation metrics.
    // 机器学习自带的api
    //    val metrics = new MulticlassMetrics(lrPredictionAndLable)
    //    val accuracy = metrics.accuracy
    //    println(s"Accuracy = $accuracy")

    //手工计算 - 计算准确率
    //    val lrTotalCorrect = data.map {
    //      r =>
    //        if (lrModel.predict(r.features) == r.label) 1 else 0
    //    }.sum
    //
    //    val lrAccuracy = lrTotalCorrect / data.count()
    //    println(s"lr model accuracy:${lrAccuracy}")
    //
    //
    //    val metrics = Seq(lrModel).map {
    //      model =>
    //        val scoreAndLabels = data.map {
    //          r =>
    //            (model.predict(r.features), r.label)
    //        }
    //        val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    //        (model.getClass.getSimpleName(), metrics.areaUnderPR(), metrics.areaUnderROC())
    //    }
    //    val allMetrics = metrics
    //    allMetrics.foreach {
    //      case (m, pr, roc) =>
    //        println(f"$m, Area under PR: ${pr * 100}%2.4f%%, Area under ROC: ${roc * 100}%2.4f%%")
    //    }


    // 模型优化
    //因为准确率只有50%左右,对二分类问题,仅比随机判断好一丢丢,所以模型的优化很有必要
    // 不要混用api 用mllib就是统统会用这个包的api 而不是ml和mllib混杂着

    //(1) 特征标准化  对所有的特征值进行标准化,统一纲量
    // 为使得数据更符合模型的假设，对每个特征进行标准化，使得每个特征是（0均值）和（单位标准差）
    //    val featuresVectors = trainingData.map(_.features)
    //    val scaler = new StandardScaler(withMean = true, withStd = true).fit(featuresVectors)
    //    val scaledData = trainingData.map(lp => LabeledPoint(lp.label, scaler.transform(lp.features)))
    //
    //    val lrModelScaled = LogisticRegressionWithSGD.train(scaledData, numIteration)
    //    val lrTotalCorrectScaled = scaledData.map { point =>
    //      if (lrModelScaled.predict(point.features) == point.label) 1 else 0
    //    }.sum()
    //
    //    val lrAccuracyScaled = lrTotalCorrectScaled / trainingData.count()
    //    val lrPredictionsVsTrue = scaledData.map { point =>
    //      (lrModelScaled.predict(point.features), point.label)
    //    }
    //
    //    val lrMetricsScaled = new BinaryClassificationMetrics(lrPredictionsVsTrue)
    //    val lrPr = lrMetricsScaled.areaUnderPR
    //    val lrRoc = lrMetricsScaled.areaUnderROC
    //    println("Normalize the training data:")
    //    println(f"${lrModelScaled.getClass.getSimpleName}\n" +
    //      f"Accuracy: ${lrAccuracyScaled * 100}%2.4f%%\nArea under PR: " +
    //      f"${lrPr * 100.0}%2.4f%%\nArea under ROC: ${lrRoc * 100.0}%2.4f%%")


    //(2) 加入类别特征
    // 前面我们只是使用了部分特征，忽略了类别变量和样板列的文本内容
    //首先，查看所有类别，并对每个类别做一个索引的映射，用1-of-k编码 one hot

    //对rdd中所有样本的第三个元素去重收集
    //这个并不是并不是按词频
    val catagories = records.map(r => r(3)).distinct.collect.zipWithIndex.toMap


    println(catagories)
    println(catagories.size)
    val numCategories = catagories.size
    val dataCatagories = records.map {
      r =>
        val trimmed = r.map(_.replaceAll("\"", ""))
        val label = trimmed(trimmed.size - 1).toInt
        // 获取单词出现的频率
        val categoryIdx = catagories(r(3))
        //      可以通过 Array.ofDim[类型](维度1, 维度2, 维度3,....)来声明多维数组
        val categoryFeatures = Array.ofDim[Double](numCategories)
        //创建一个长为14的向量来表示类别特征，然后根据每个样本所属类别索引，对相应的维度赋值为1，其他为0
        //没有初始化的Double类型数组,默认填充0.0
        categoryFeatures(categoryIdx) = 1.0
        val otherFeatures = trimmed.slice(4, trimmed.size - 1).map(x => if (x == "?") 0.0 else x.toDouble)
        val features = categoryFeatures ++ otherFeatures
        LabeledPoint(label, Vectors.dense(features))
    }

    println(dataCatagories.first())
    // 文章类别
    // (0.0,[0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.789131,2.055555556,0.676470588,0.205882353,0.047058824,0.023529412,0.443783175,0.0,0.0,0.09077381,0.0,0.245831182,0.003883495,1.0,1.0,24.0,0.0,5424.0,170.0,8.0,0.152941176,0.079129575])

    val featuresVectorsCats = dataCatagories.map(_.features)
    val scalerCats = new StandardScaler(withMean = true, withStd = true).fit(featuresVectorsCats)
    val scaledDataCats = dataCatagories.map(lp => LabeledPoint(lp.label, scalerCats.transform(lp.features)))
    println(scaledDataCats.first())

    val lrModelScaledCats = LogisticRegressionWithSGD.train(scaledDataCats, numIteration)
    val lrTotalCorrectScaledCats = scaledDataCats.map { point =>
      if (lrModelScaledCats.predict(point.features) == point.label) 1 else 0
    }.sum()

    val lrAccuracyScaledCats = lrTotalCorrectScaledCats / dataCatagories.count()
    val lrPredictionsVsTrueCats = scaledDataCats.map { point =>
      (lrModelScaledCats.predict(point.features), point.label)
    }

    val lrMetricsScaledCat = new BinaryClassificationMetrics(lrPredictionsVsTrueCats)
    val lrPrcats = lrMetricsScaledCat.areaUnderPR
    val lrRocCats = lrMetricsScaledCat.areaUnderROC
    println("Normalize the training data:")
    println(
      f"${lrModelScaledCats.getClass.getSimpleName}\n" +
        f"Accuracy: ${lrAccuracyScaledCats * 100}%2.4f%%\nArea under PR: " +
        f"${lrPrcats * 100.0}%2.4f%%\nArea under ROC: ${lrRocCats * 100.0}%2.4f%%"
    )


    //定义辅助函数,根据给定输入训练模型
    def trainWithParams(input: RDD[LabeledPoint], regParam: Double, numIterations: Int, updater: Updater, stepSize: Double) = {
      val lr = new LogisticRegressionWithSGD()
      lr.optimizer.setNumIterations(numIteration)
        .setUpdater(updater)
        .setRegParam(regParam)
        .setStepSize(stepSize)
      lr.run(input)
    }

    //定义第二个辅助函数,根据输入的数据和分类迷行,计算相关的AUC
    def createMetrics(iter: String, data: RDD[LabeledPoint], model: ClassificationModel) = {
      val scoreAndLabels = data.map {
        point =>
          (model.predict(point.features), point.label)
      }

      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (iter, metrics.areaUnderROC())
    }

    //为了加速多次模型训练的速度,可以缓存标准化的数据
    // 两种写法在scala都是正确的,即如果函数没有参数,name函数后面的()可以省略
    scaledDataCats.cache
    //    scaledDataCats.cache()


    //-迭代次数 这里设置不同的迭代次数numIterations，然后比较AUC结果
    //-思路就是其他参数设置初始值,对一个参数对此调整 查看模型表现 performance

    val iterResults = Seq(1, 5, 10, 50).map { param =>
      val model = trainWithParams(scaledDataCats, 0.0, param, new
          SimpleUpdater, 1.0)
      createMetrics(s"$param iterations", scaledDataCats, model)
    }
    iterResults.foreach { case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.2f%%") }



    //-步长
    //    在SGD中，在训练每个样本并更新模型的权重向量时，步长用来控制算法在最陡的梯度方向上应该前进多远。较大的步长收敛较快，但是步长太大可能导致收敛到局部最优解

    val stepResults = Seq(0.001, 0.01, 0.1, 1.0, 10.0).map { param =>
      val model = trainWithParams(scaledDataCats, 0.0, 100, new SimpleUpdater, param)
      createMetrics(s"$param step size", scaledDataCats, model)
    }
    stepResults.foreach { case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.2f%%") }


    // 正则化的目的  限制模型的复杂度避免模型在训练数据中过拟合
    //正则化 前面逻辑回归的代码中简单提及了Updater类，该类在MLlib中实现了正则化。正则化通过
    //限制模型的复杂度避免模型在训练数据中过拟合
    val regResults = Seq(0.001, 0.01, 0.1, 1.0, 10.0).map { param =>
      val model = trainWithParams(scaledDataCats, param, 100,
        new SquaredL2Updater, 1.0)
      createMetrics(s"$param L2 regularization parameter",
        scaledDataCats, model)
    }
    regResults.foreach { case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.2f%%") }



    //// 以上测试 模型都是基于线性模型的预测,下面测试使用非线性的模型 ////

    //引入类并创建辅助函数
    def trainDTWithParams(input: RDD[LabeledPoint], maxDepth: Int, impurity: Impurity) = {
      // 直接返回决策树模型
      DecisionTree.train(input, Algo.Classification, impurity, maxTreeDepth)
    }


    // 首先，通过使用Entropy不纯度并改变树的深度训练模型
    val dtResultsEntropy = Seq(1, 2, 3, 4, 5, 10, 20).map { param =>
      val model = trainDTWithParams(data, param, Entropy)
      val scoreAndLabels = data.map { point =>
        val score = model.predict(point.features)
        (if (score > 0.5) 1.0 else 0.0, point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (s"$param tree depth", metrics.areaUnderROC)
    }

    dtResultsEntropy.foreach { case (param, auc) => println(f"$param,AUC = ${auc * 100}%2.2f%%") }

    // 接下来，我们采用Gini不纯度进行类似的计算
    val dtResultsGini = Seq(1, 2, 3, 4, 5, 10, 20).map { param =>
      val model = trainDTWithParams(data, param, Gini)
      val scoreAndLabels = data.map { point =>
        val score = model.predict(point.features)
        (if (score > 0.5) 1.0 else 0.0, point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (s"$param tree depth", metrics.areaUnderROC)
    }
    dtResultsGini.foreach { case (param, auc) => println(f"$param,AUC = ${auc * 100}%2.2f%%") }


  }
}
