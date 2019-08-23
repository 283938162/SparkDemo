package com.spark.ml.官方学习案例

//旧版导包
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.regression
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.ml.feature.LabeledPoint
//import org.apache.spark.mllib.regression.LabeledPoint


/**
  * https://dongkelun.com/2018/04/09/sparkMlLinearRegressionUsing/
  */


object LinearRegressionNewAndOldApi {

  // 推荐使用这个屏蔽日志 比下面这个更彻底
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  //  Logger.getRootLogger("org.apache.spark").setLevel(Level.WARN)


  def newApi(): Unit = {

  }

  /**
    * 获取普通标签格式通过RDD的形式
    * 旧版（mllib）的线性回归要求传入的参数类型为RDD[LabeledPoint]
    * 标签格式为
    * 标签,特征值1 特征值2 特征值3...
    * ---------------------------------
    * 1,1.9
    * 2,3.1
    * 3,4
    * ---------------------------------
    */
  def getGeneralLabelFormatByRDD(sc: SparkContext) = {
    val data_path = "input/linear_regression_data1.txt"
    val data = sc.textFile(data_path)

    //  方法(1)
    //    val training = data.map {
    //      line =>
    //        val label = line.split(",")(0).toDouble
    //        val features = line.split(",")(1).split(" ").map(_.toDouble)
    //        //val features = line.split(",")(1).split(" ").map(f => f.toDouble)
    //        LabeledPoint(label, Vectors.dense(features)) //Vectors.dense(Array[Double])
    //    }

    //方法(2)

    //    val training = data.map {
    //      line =>
    //        val arr = line.split(",")
    //        LabeledPoint(arr(0).toDouble, Vectors.dense(arr(1).split(" ").map(_.toDouble))) //Vectors.dense(Array[Double])
    //    }

    //方法(3)

    var training = data.map(
      line => LabeledPoint(
        line.split(",")(0).toDouble,
        Vectors.dense(line.split(",")(1).split(" ").map(_.toDouble))
      )
    )
    training
  }

  //  一共有两列，第一列可以通过.label获得（类型为Double），第二列可以通过.features获得（类型为Vector[Double]）
  //  (1.0,[1.9,3.4,4.5,6.0])
  //  (2.0,[3.1])
  //  (3.0,[4.0])
  //  (3.5,[4.45])
  //  (4.0,[5.02])
  //  (9.0,[9.97])
  //  (-2.0,[-0.98])


  /**
    * 新版以Dataframe为基础
    * 新版（ml）的线性回归要求传入的参数类型为Dataset[_]
    */
  def getGeneralLabelFormatByDataframe(spark: SparkSession) = {

    import spark.implicits._
    val data_path = "input/linear_regression_data1.txt"
    val data: DataFrame = spark.read.text(data_path)
    //    data.show()
    //    data.map(_+"hello").show()

    // 为啥要使用case Row呢?
    //    spark2.0 DataSet数据集在使用sql()时，无法使用map，flatMap等转换算子的解决办法
    //    其中一种解决方案:
    //      直接利用scala的模式匹配的策略case Row来进行是可以通过的，原因是case Row() scala模式匹配的知识，
    //    这样可以知道集合Row里面拥有多少个基本的类型，则可以通过scala就可以完成对Row的自动编码，然后可以进行相应的处理。
    //    参考:https://blog.51cto.com/9269309/1954540
    //   方法一
    // val training = data.map {
    //      case Row(line: String) =>
    //                val arr = line.split(",")
    //                (arr(0).toDouble, Vectors.dense(arr(1).split(" ").map(_.toDouble)))
    //    }
    //      .toDF("label", "features")

    // 不使用case Row 无法识别line是String
    // 不使用case Row 使用scala对字符串的处理
    //    (1)方法二
    val training = data.map {
      line =>
        val arrline = line.toString().substring(1, line.toString().length - 1)
        val arr = arrline.split(",")
        (arr(0).toDouble, Vectors.dense(arr(1).split(" ").map(_.toDouble)))
    }.toDF()

    //
    //
    training


  }


  def getLibSVMLabelFormatByRDD(sc: SparkContext) = {
    val data_path = "input/linear_regression_data2.txt"
    val data = MLUtils.loadLibSVMFile(sc, data_path)
    data
  }

  def getLibSVMLabelFormatByDataframe(spark: SparkSession) = {
    import spark.implicits._
    val data_path = "input/linear_regression_data2.txt"
    val data: DataFrame = spark.read.format("libsvm").load(data_path)
    data
  }

  def oldApi() = {
    val conf = new SparkConf().setAppName("").setMaster("local")
    val sc = new SparkContext(conf)

    //    val training = getGeneralLabelFormatByRDD(sc).cache()
    //    training.foreach(println)
    //    val training = getLibSVMLabelFormatByRDD(sc)
    //    training.foreach(println)

    //    val spark = SparkSession.builder().master("local").getOrCreate()
    //    val training = getGeneralLabelFormatByDataframe(spark)
    //    training.show()

    val spark = SparkSession.builder().master("local").getOrCreate()
    val training = getLibSVMLabelFormatByDataframe(spark)
    training.show()


  }

  def oldLinearRegression(): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    // 构建Spark对象
    val conf = new SparkConf().setAppName("OldLinearRegression").setMaster("local")
    val sc = new SparkContext(conf)

    //读取样本数据
    val data_path = "data/mllib/sample_libsvm_data.txt"
    val training = MLUtils.loadLibSVMFile(sc, data_path).cache()
    val numTraing = training.count()

    // 新建线性回归模型，并设置训练参数
    val numIterations = 10000
    val stepSize = 0.5
    val miniBatchFraction = 1.0 // 每次迭代参与计算的样本比例，默认为1.0 即全部参与迭代

    //书上的代码 intercept 永远为0
    //val model = LinearRegressionWithSGD.train(examples, numIterations, stepSize, miniBatchFraction)
    var lr = new LinearRegressionWithSGD().setIntercept(true)
    lr.optimizer.setNumIterations(numIterations).setStepSize(stepSize).setMiniBatchFraction(miniBatchFraction)
    val model = lr.run(training)
    println(model.weights)
    println(model.intercept)


    // 对样本进行测试
    val prediction = model.predict(training.map(_.features))
    val predictionAndLabel = prediction.zip(training.map(_.label))

    val print_predict = predictionAndLabel.take(20)
    println("prediction" + "\t" + "label")
    for (i <- 0 to print_predict.length - 1) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }

    // 计算测试误差
    val loss = predictionAndLabel.map {
      case (p, l) =>
        val err = p - l
        err * err
    }.reduce(_ + _)
    val rmse = math.sqrt(loss / numTraing)
    println(s"Test RMSE = $rmse.")
  }

  def newLinearRegression(): Unit = {
    val spark = SparkSession
      .builder
      .appName("NewLinearRegression")
      .master("local")
      .getOrCreate()
    val data_path = "data/mllib/sample_libsvm_data.txt"

    import spark.implicits._
    import org.apache.spark.ml.linalg.Vectors
    import org.apache.spark.sql.Row
    val training = spark.read.format("libsvm").load(data_path)

    val lr = new LinearRegression()
      .setMaxIter(10000)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val lrModel = lr.fit(training)

    //回归评价指标MSE、RMSE、MAE、R-Squared

    //    https://www.jianshu.com/p/9ee85fdad150

    // 输出回归系数和模型偏执
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Extract the summary from the returned LogisticRegressionModel instance trained in the earlier example
    // 也就是说模型训练的结果的概要都保存到这个参数里了
    val trainingSummary = lrModel.summary


    // 获取迭代次数
    println(s"numIterations: ${trainingSummary.totalIterations}")

    // obtain the loss per iteration
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")

    //residuals(pre)     #残差
    trainingSummary.residuals.show()

    //均方根误差
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")

    //R Squared
    println(s"r2: ${trainingSummary.r2}")


    trainingSummary.predictions.show()


    spark.stop()
  }

  def main(args: Array[String]): Unit = {
    //    oldApi()
    //    newApi()

    oldLinearRegression()
    //    newLinearRegression()

  }
}
