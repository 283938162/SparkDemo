package com.spark.ml

import org.apache.spark.ml._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * Spark 2.0 机器学习 ML 库：常见的机器学习模型（Scala 版）
  * https://blog.csdn.net/larger5/article/details/81707571
  *
  * 关于SparkMLlib的基础数据结构Spark-MLlib-Basics
  * https://blog.csdn.net/canglingye/article/details/41316193
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

  }
}
