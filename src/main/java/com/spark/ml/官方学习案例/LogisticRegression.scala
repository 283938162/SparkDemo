package com.spark.ml.官方学习案例

package Classification

/**
  * LogisticRegression Algorithm
  * Created by wy on 2019/03/25
  */

//spark初始化
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
//分类数据格式处理
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
//逻辑回归-随机梯度下降SGD
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
//计算Accuracy、PR、ROC和AUC
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
//数据标准化
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.feature.StandardScaler
//参数调优
import org.apache.spark.mllib.optimization.{Updater, SimpleUpdater, L1Updater, SquaredL2Updater}
import org.apache.spark.mllib.classification.ClassificationModel

object LogisticRegression {

  //屏蔽不必要的日志显示在终端上
  //Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR) //warn类信息不会显示，只显示error级别的
  Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {
    //初始化
    val conf = new SparkConf().setMaster("local").setAppName("LogisticRegression")
    val sc = new SparkContext(conf)
    /**
      * 数据：lr_test.txt
      * 该数据集包含了46个feature,1个label
      **/
    //input
    // 如果不是标准数据格式  第一步就是将数据集转化成一个LabledPoint
    //    val sourceRDD = sc.textFile("E:\\Spark\\scala-data\\LRdata\\lr_test.txt")
    //    val data = sourceRDD.map{
    //      line =>{
    //        val arr = line.split("#")
    //        val label = arr(1).toDouble
    //        val features = arr(0).split(",").map(_.toDouble)
    //        LabeledPoint(label,Vectors.dense(features))  //创建一个稠密向量
    //      }
    //    }

    //直接返回 RDD[LabeledPoint]
    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")

    /**
      * 创建一个稀疏向量（第一种方式）
      * val sv1: Vector = Vector.sparse(3, Array(0,2), Array(1.0,3.0));
      * 创建一个稀疏向量（第二种方式）
      * val sv2 : Vector = Vector.sparse(3, Seq((0,1.0),(2,3.0)))
      *
      * 对于稠密向量：很直观，你要创建什么，就加入什么，其函数声明为Vector.dense(values : Array[Double])
      * 对于稀疏向量，当采用第一种方式时，3表示此向量的长度，第一个Array(0,2)表示的索引，第二个Array(1.0, 3.0)
      * 与前面的Array(0,2)是相互对应的，表示第0个位置的值为1.0，第2个位置的值为3
      *
      * 对于稀疏向量，当采用第二种方式时，3表示此向量的长度，后面的比较直观，Seq里面每一对都是(索引，值）的形式。注意Array 与 Seq
      **/
    data.cache() //缓存
    val Array(trainData, testData) = data.randomSplit(Array(0.8, 0.2), seed = 11L)
    trainData.cache()
    testData.cache()
    val numData = data.count
    val numTrainData = trainData.count
    val numTestData = testData.count

    val numFeatures = trainData.first().features.size
    println("原始数据量：", numData) //100
    println("训练数据量：", numTrainData) //83
    println("测试数据量：", numTestData) //17
    println("特征值为度：", numFeatures) //17




    val stepSize = 0.1 //迭代步长，默认为1.0
    val numIterations = 50 //迭代次数，默认为100
    val miniBatchFraction = 1.0 //每次迭代参与计算的样本比例，默认为1.0

    /** 训练逻辑回归模型 */
    val lrModel = LogisticRegressionWithSGD.train(data, numIterations, stepSize, miniBatchFraction)
    //打印模型权重值
    val res = lrModel.weights.toArray
    println("权重值列表如下：")
    res.foreach(println)
    println("----------------------拟合预测结果--------------------")

    //预测值与真实值比较  如何取值?  first取RDD的第一个行,这一行是LabeledPoint格式,包含(label,features)
    //val testPoint = testData.first
    //val testPredict = lrModel.predict(testPoint.features)
    //testPredict: Double = 0.0
    //val testTrueLabel = testPoint.label
    //testTrueLabel: Double = 0.0

    /** 预测的正确率计算 */
    val lrTestCorrect = data.map { x =>
      if (lrModel.predict(x.features) == x.label) 1 else 0
    }.sum
    //预测正确率
    val lrAccuracy = lrTestCorrect / numData
    println(f"Accuracy：${lrAccuracy * 100}%2.3f%%")
    // Accuracy: 97.839%

    /** 计算 准确率-召回律(PR曲线) ROC曲线的面积(AUC)
      * 1.准确率通常用于评价结果的质量，定义为真阳性的数目除以真阳性和假阳性的总数，其中真阳性值被预测的类别为1的样本，
      * 假阳性是错误预测为1的样本。
      * 2.召回率用来评价结果的完整性，定义为真阳性的数目除以真阳性和假阳性的和，其中假阳性是类别为1却被预测为0的样本。
      * 通常高准确率对应着低召回率
      * 3.ROC曲线与PR曲线类似，是对分类器的真阳性率-假阳性率的图形化解释。
      * */
    val metrics = Seq(lrModel).map { model =>
      val scoreAndLabels = data.map { x =>
        (model.predict(x.features), x.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (model.getClass.getSimpleName, metrics.areaUnderPR(), metrics.areaUnderROC())
    }

    //val allMetrics = metrics ++ nbMetrics ++ dtMetrics
    metrics.foreach { case (model, pr, roc) =>
      println(f"model:$model\n" +
        f"Area under PR: ${pr * 100.0}%2.3f%%\n" +
        f"Area under ROC: ${roc * 100.0}%2.3f%%")
    }
    //Accuracy：97.839%
    //model:LogisticRegressionModel
    //Area under PR: 51.081%
    //Area under ROC: 50.000%

    /** 改进模型性能以及参数调优 */
    //特征标准化
    //将特征变量用（RowMatrix类）表示成MLlib中的（分布矩阵）
    val vectors = data.map(x => x.features)
    val matrix = new RowMatrix(vectors)
    val matrixSummary = matrix.computeColumnSummaryStatistics() //计算矩阵每列的统计特性
    println("----------------------特征标准化----------------------")
    println("mean:       ", matrixSummary.mean) //输出每列均值
    println("max:        ", matrixSummary.max) //每列最大值
    println("variance:   ", matrixSummary.variance) //矩阵每列方差
    println("numNonzeros:", matrixSummary.numNonzeros) //每列非0项的数目
    println("normL2:     ", matrixSummary.normL2) //L2范数：向量各元素的平方和然后求平方根

    /** 为使得数据更符合模型的假设，对每个特征进行标准化，使得每个特征是（0均值）和（单位标准差） */
    //做法：对（每个特征值）减去（列的均值），然后（除以）列的（标准差）以进行缩放
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(vectors) //将向量传到转换函数
    val scaledData = data.map(x => LabeledPoint(x.label, scaler.transform(x.features)))

    //println(data.first.features)
    println("标准化后的特征第一行结果：")
    println(scaledData.first.features)
    //[0.0016544159298287912,0.0273303020874253,0.008141541536538578,0.07992614623509364,...
    //为验证第一个特征已经应用标准差公式被转换了，用 第一个特征（减去）其均值，然后（除以）标准差--方差的平方根
    println("验证第一个特征是否正确")
    println((data.first.features(0) - matrixSummary.mean(0)) / math.sqrt(matrixSummary.variance(0)))
    //0.0016544159298287912 验证正确

    /** 现在使用标准化的数据重新训练模型逻辑回归-（决策树和朴素贝叶斯不受特征标准化的影响） */
    //val Array(scaledTrainData, scaledTestData) = scaledData.randomSplit(Array(0.8,0.2),seed = 11L)

    val scaledLrModel = LogisticRegressionWithSGD.train(scaledData, numIterations, stepSize, miniBatchFraction)

    val scaledLrCorrect = scaledData.map { x =>
      if (scaledLrModel.predict(x.features) == x.label) 1 else 0
    }.sum
    val scaledLrTestAccuracy = scaledLrCorrect / numData

    val lrPredictionsVsTrue = scaledData.map { x =>
      (scaledLrModel.predict(x.features), x.label)
    }
    val lrMetricsScaled = new BinaryClassificationMetrics(lrPredictionsVsTrue)
    val lrPr = lrMetricsScaled.areaUnderPR() //lrPr: Double = 0.27532
    val lrPoc = lrMetricsScaled.areaUnderROC() //lrPoc: Double = 0.58451

    println("------------标准化后数据训练、拟合和结果----------------")
    println(f"Model:${scaledLrModel.getClass.getSimpleName}\n" +
      f"Accuracy:      ${scaledLrTestAccuracy * 100}%2.3f%%\n" +
      f"Area under PR: ${lrPr * 100}%2.3f%%\n" +
      f"Area under ROC:${lrPoc * 100}%2.3f%%")
    //Model:LogisticRegressionModel
    //Accuracy:      64.974%
    //Area under PR: 35.237%
    //Area under ROC:65.355%

    /** 模型参数调优MLlib线性模型优化技术：SGD和L-BFGS(只在逻辑回归中使用LogisticRegressionWithLBFGS) */
    //线性模型
    //定义训练调参辅助函数，根据给定输入训练模型 (输入， 则正则化参数， 迭代次数， 正则化形式， 步长)
    def trainWithParams(input: RDD[LabeledPoint], regParam: Double, numIterations: Int,
                        updater: Updater, stepSize: Double) = {
      val lr = new LogisticRegressionWithSGD //逻辑回归也可以用LogisticRegressionWithLBFGS
      lr.optimizer
        .setNumIterations(numIterations) //迭代次数
        .setStepSize(stepSize) //步长
        .setRegParam(regParam) //则正则化参数
        .setUpdater(updater) //正则化形式
      lr.run(input) //输入训练数据RDD
    }

    //定义第二个辅助函数,label为需要调试的参数，data:输入预测的数据，model训练的模型
    def createMetrics(label: Double, data: RDD[LabeledPoint], model: ClassificationModel) = {
      val scoreAndLabels = data.map { point =>
        (model.predict(point.features), point.label) //(predicts,label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (label, metrics.areaUnderROC()) //计算AUC
    }

    //加快多次模型训练速度, 缓存标准化后的数据
    scaledData.cache()
    println("------------------标准化后数据调参---------------------")
    //1迭代次数
    val iterateResults = Seq(1, 5, 10, 50, 100).map { param =>
      //训练
      val model = trainWithParams(scaledData, 0.0, param, new SimpleUpdater, 1.0)
      //拟合，计算AUC
      createMetrics(param, scaledData, model)
    }
    println("1迭代次数numIterations：Seq(1, 5, 10, 50, 100)")
    iterateResults.foreach { case (param, auc) => println(f"$param iterations, AUC = ${auc * 100}%2.2f%%") }
    //1 iterations, AUC = 64.50%
    //5 iterations, AUC = 67.07%
    //10 iterations, AUC = 67.10%
    //50 iterations, AUC = 67.56%
    //100 iterations, AUC = 67.56%
    var maxIterateAuc = 0.0
    var bestIterateParam = 0
    for (x <- iterateResults) {
      //println(x)
      if (x._2 > maxIterateAuc) {
        maxIterateAuc = x._2
        bestIterateParam = x._1.toInt
      }
    }
    println("max auc: " + maxIterateAuc + " best numIterations param: " + bestIterateParam)


    //2步长 大步长收敛快，太大可能导致收敛到局部最优解
    val stepResults = Seq(0.001, 0.01, 0.1, 1.0, 10.0).map { param =>
      val model = trainWithParams(scaledData, 0.0, bestIterateParam, new SimpleUpdater, param)
      createMetrics(param, scaledData, model)
    }
    println("\n2步长stepSize：Seq(0.001, 0.01, 0.1, 1.0, 10.0)")
    stepResults.foreach { case (param, auc) => println(f"$param stepSize, AUC = ${auc * 100}%2.2f%%") }
    //0.001 step size, AUC = 64.50%
    //0.01 step size, AUC = 64.50%
    //0.1 step size, AUC = 65.36%
    //1.0 step size, AUC = 67.56%
    //10.0 step size, AUC = 50.20%
    var maxStepAuc = 0.0
    var bestStepParam = 0.0
    for (x <- stepResults) {
      //println(x)
      if (x._2 > maxStepAuc) {
        maxStepAuc = x._2
        bestStepParam = x._1
      }
    }
    println("max auc: " + maxStepAuc + " best stepSize param: " + bestStepParam)

    //3.1正则化参数，默认值为0.0，L1正则new L1Updater
    val regL1Results = Seq(0.0, 0.001, 0.01, 0.1, 1.0, 10.0).map { param =>
      val model = trainWithParams(scaledData, param, bestIterateParam, new L1Updater, bestStepParam)
      createMetrics(param, scaledData, model)
    }
    println("\n3.1 L1正则化参数regParam：Seq(0.0, 0.001, 0.01, 0.1, 1.0, 10.0)")
    regL1Results.foreach { case (param, auc) => println(f"$param regParam L1, AUC = ${auc * 100}%2.2f%%") }
    //regParam L1 = 0.0, AUC = 67.56%
    //regParam L1 = 0.001, AUC = 66.43%
    //regParam L1 = 0.01, AUC = 65.74%
    //regParam L1 = 0.1, AUC = 50.00%
    //regParam L1 = 1.0, AUC = 50.00%
    //regParam L1 = 10.0, AUC = 50.00%
    var maxRegL1Auc = 0.0
    var bestRegL1Param = 0.0
    for (x <- regL1Results) {
      //println(x)
      if (x._2 > maxRegL1Auc) {
        maxRegL1Auc = x._2
        bestRegL1Param = x._1
      }
    }
    println("max auc: " + maxRegL1Auc + " best L1regParam: " + bestRegL1Param)

    //3.2正则化参数：默认值为0.0，L2正则new SquaredL2Updater
    val regL2Results = Seq(0.0, 0.001, 0.01, 0.1, 1.0, 10.0).map { param =>
      val model = trainWithParams(scaledData, param, bestIterateParam, new SquaredL2Updater, bestStepParam)
      createMetrics(param, scaledData, model)
    }
    println("\n3.2 L2正则化参数regParam：Seq(0.0, 0.001, 0.01, 0.1, 1.0, 10.0)")
    regL2Results.foreach { case (param, auc) => println(f"$param regParam L2, AUC = ${auc * 100}%2.2f%%") }
    //regParam L2 = 0.0 , AUC = 67.56%
    //regParam L2 = 0.001 , AUC = 67.56%
    //regParam L2 = 0.01 , AUC = 67.43%
    //regParam L2 = 0.1 , AUC = 67.14%
    //regParam L2 = 1.0 , AUC = 66.60%
    //regParam L2 = 10.0 , AUC = 36.76%
    var maxRegL2Auc = 0.0
    var bestRegL2Param = 0.0
    for (x <- regL2Results) {
      //println(x)
      if (x._2 > maxRegL2Auc) {
        maxRegL2Auc = x._2
        bestRegL2Param = x._1
      }
    }
    println("max auc: " + maxRegL2Auc + " best L2regParam: " + bestRegL2Param)
    //4正则化形式：默认为new SimpleUpdater 正则化系数无效，前两个参数调参后最优AUC为maxStepAuc
    //则，3.1和3.2的最优AUC与maxStepAuc比较，较大的则为最优正则化形式
    var bestRegParam = 0.0
    var bestUpdaterID = 0
    if (maxStepAuc >= maxRegL1Auc) {
      if (maxStepAuc >= maxRegL2Auc) {
        bestUpdaterID = 0
        bestRegParam = 0.0
      }
      else {
        bestUpdaterID = 2
        bestRegParam = bestRegL2Param
      }
    }
    else {
      if (maxRegL2Auc >= maxRegL1Auc) {
        bestUpdaterID = 2
        bestRegParam = bestRegL2Param
      }
      else {
        bestUpdaterID = 1
        bestRegParam = bestRegL1Param
      }
    }
    val Updaters = Seq(new SimpleUpdater, new L1Updater, new SquaredL2Updater)
    val bestUpdater = Updaters(bestUpdaterID)

    //最优参数:
    println("------------------更新模型训练参数---------------------")
    println(f"best numIterations param: $bestIterateParam\n" +
      f"best stepSize param: $bestStepParam\n" +
      f"best regParam: $bestRegParam\n" +
      f"best regUpdater: $bestUpdater\n"
    )
    // numIterations:50
    // stepSize:1.0
    // regParam:0.0
    // updater:new SimpleUpdater
    /** 数据标准化、参数调优后，再次训练逻辑回归模型，可以用28分训练测试 */
    val upDateLrModel = trainWithParams(scaledData, bestRegParam, bestIterateParam, bestUpdater, bestStepParam)

    //保存和加载模型
    upDateLrModel.save(sc, "E:\\Spark\\scala-data\\Model")
    val newModel = LogisticRegressionModel.load(sc, "E:\\Spark\\scala-data\\Model")

    //打印模型权重值
    val newRes = newModel.weights.toArray
    println("参数调优后特征权重值列表如下：")
    newRes.foreach(println)

  }
}
