package com.spark.ml

import com.test.CommonUtils
import org.apache.spark.ml._
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * Spark 2.0 机器学习 ML 库：常见的机器学习模型（Scala 版）
  * https://blog.csdn.net/larger5/article/details/81707571
  *
  * 关于SparkMLlib的基础数据结构Spark-MLlib-Basics
  * https://blog.csdn.net/canglingye/article/details/41316193
  *
  * 逻辑回归本质上也是一种线性回归，和普通线性回归不同的是，普通线性回归特征到结果输出的是连续值，
  * 而逻辑回归增加了一个函数g(z)，能够把连续值映射到0或者1。
  *
  * 优秀的博文
  * https://blog.csdn.net/jediael_lu/article/details/76509707
  */
object LogicTest {

  def main(args: Array[String]): Unit = {
    // 0. 构造Spark对象
    val spark = SparkSession
      .builder()
      .master("local[2]") // 本地测试，服务器部署需要注释掉
      .appName("LogicTest")
      //      .enableHiveSupport()  //连接hive元数据库
      .getOrCreate() //有就获取无则创建

    //设置文件读取、存储的目录，HDFS最佳
    //spark 程序都需要建立checkpoint目录吗?
    spark.sparkContext.setCheckpointDir("checkpoint")

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    /**
      * ==本地向量==
      * //创建一个稀疏向量（第一种方式）
      * val sv1: Vector = Vector.sparse(3, Array(0,2), Array(1.0,3.0));
      * //创建一个稀疏向量（第二种方式）
      * val sv2 : Vector = Vector.sparse(3, Seq((0,1.0),(2,3.0)))
      * *
      * 对于稀疏向量，当采用第一种方式时，3表示此向量的长度，第一个Array(0,2)表示的索引，第二个Array(1.0, 3.0)与前面的Array(0,2)是相互对应的，
      * 表示第0个位置的值为1.0，第2个位置的值为3
      * 对于稀疏向量，当采用第二种方式时，3表示此向量的长度，后面的比较直观，Seq里面每一对都是(索引，值）的形式。
      * *
      * tips:由于scala中会默认包含scal.collection.immutalbe.Vector，所以当使用MLlib中的Vector时，需要显式的指明import路径
      *
      * ==向量标签==
      * 向量标签和向量是一起的，简单来说，可以理解为一个向量对应的一个特殊值，这个值的具体内容可以由用户指定，比如你开发了一个算法A，
      * 这个算法对每个向量处理之后会得出一个特殊的标记值p，你就可以把p作为向量标签。
      * 同样的，更为直观的话，你可以把向量标签作为行索引，从而用多个本地向量构成一个矩阵（当然，MLlib中已经实现了多种矩阵）
      *
      * (向量标签,向量)
      * (1.0, Vectors.sparse(692, Array(10, 20, 30), Array(-1.0, 1.5, 1.3))),
      *
      */

    // 1.训练样本准备 创建稀疏矩阵
    val training = spark.createDataFrame(Seq(
      //向量标签,向量
      (1.0, Vectors.sparse(692, Array(10, 20, 30), Array(-1.0, 1.5, 1.3))),
      (0.0, Vectors.sparse(692, Array(45, 175, 500), Array(-1.0, 1.5, 1.3))),
      (1.0, Vectors.sparse(692, Array(100, 200, 300), Array(-1.0, 1.5, 1.3))))).toDF("label", "features")



    // flase 不折叠数据
    training.show(false)

    /**
      *
      * +-----+----------------------------------+
      * |label|features                          |
      * +-----+----------------------------------+
      * |1.0  |(692,[10,20,30],[-1.0,1.5,1.3])   |
      * |0.0  |(692,[45,175,500],[-1.0,1.5,1.3]) |
      * |1.0  |(692,[100,200,300],[-1.0,1.5,1.3])|
      * +-----+----------------------------------+
      */

    //建立逻辑回归模型,设置回归模型参数
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    //根据训练样本进行模型训练  fit的入参是 DF DF里面的Row是(向量标签,向量)
    val lrModel = lr.fit(training);

    // 打印模型信息
    println(s"Coefficients: ${lrModel.coefficients},Intercept: ${lrModel.intercept}")
    //    println("Coefficients:"+lrModel.coefficients+",intercept:"+lrModel.intercept)

    /**
      * Coefficients:(692,[45,175,500],[0.48944928041408226,-0.32629952027605463,-0.37649944647237077]),
      * intercept:1.251662793530725
      *
      */

    //3 建立多元回归模型
    val mlr = new LogisticRegression()
      .setMaxIter(10) // 最大的迭代次数，当达到这个次数时，不管是否已经收敛到最小误差，均会结束训练。默认值为100。
      .setRegParam(0.3) //setRegParam：正则化参数
      .setElasticNetParam(0.8) // 默认值为0.0，这是一个L2惩罚。用于防止过拟合的另一种方式
      .setFamily("multinomial") // 这是2.1才引入的参数，可设置二项式还是多项式模型。

    //3 根据训练样本进行模型训练
    val mlrModel = mlr.fit(training)

    //3 打印模型信息
    println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")
    println(s"Multinomial intercepts: ${mlrModel.interceptVector}")

    /**
      * Multinomial coefficients: 2 x 692 CSCMatrix
      * (0,45) -0.2610332862832527
      * (1,45) 0.2610332862832519
      * (0,175) 0.17402219085550177
      * (1,175) -0.17402219085550127
      * (0,500) 0.20079483560250153
      * (1,500) -0.20079483560250128
      * Multinomial intercepts: [-0.6449310568167714,0.6449310568167714]
      */

    //4 测试样本
    val test = spark.createDataFrame(Seq(
      (1.0, Vectors.sparse(692, Array(10, 20, 30), Array(-1.0, 1.5, 1.3))),
      (0.0, Vectors.sparse(692, Array(45, 175, 500), Array(-1.0, 1.5, 1.3))),
      (1.0, Vectors.sparse(692, Array(100, 200, 300), Array(-1.0, 1.5, 1.3))))).toDF("label", "features")
    test.show(false)

    //5 对模型进行测试 返回的是DF格式  包含如下的字段
    val test_predict = lrModel.transform(test)
    test_predict
      .show(false)

    /**
      * probablity 概率 [A,B]  A代表不是改标签的概率 B代表是该标签的概率 A+B=1
      * +-----+----------------------------------+----------------------------------------+----------------------------------------+----------+
      * |label|features                          |rawPrediction                           |probability                             |prediction|
      * +-----+----------------------------------+----------------------------------------+----------------------------------------+----------+
      * |1.0  |(692,[10,20,30],[-1.0,1.5,1.3])   |[-1.251662793530725,1.251662793530725]  |[0.22241243403014824,0.7775875659698517]|1.0       |
      * |0.0  |(692,[45,175,500],[-1.0,1.5,1.3]) |[0.2166850477115212,-0.2166850477115212]|[0.5539602964649871,0.44603970353501293]|0.0       |
      * |1.0  |(692,[100,200,300],[-1.0,1.5,1.3])|[-1.251662793530725,1.251662793530725]  |[0.22241243403014824,0.7775875659698517]|1.0       |
      * +-----+----------------------------------+----------------------------------------+----------------------------------------+----------+
      */

    // 模型调优

    //模型的保存与加载
//    lrModel.save("ml_model/lrmodel")

    val load_lrModel = LogisticRegressionModel.load("ml_model/lrmodel")
    load_lrModel.transform(test).show(false)


  }
}
