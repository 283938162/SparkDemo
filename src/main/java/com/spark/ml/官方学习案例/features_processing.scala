package com.spark.ml.官方学习案例

import org.apache.log4j.{Level, Logger}
import org.apache.log4j.spi.LoggerFactory
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.SparkSession

/**
  * 特征工程
  * https://blog.csdn.net/chenguangchun1993/article/details/78840577
  */
object features_processing {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def testStringIndexer(): Unit = {
    val spark = SparkSession.builder().master("local").appName("StringIndexer").getOrCreate()

    //构造DF 一般都是喜欢使用Seq
    val df = spark.createDataFrame(
      Seq(
        (0, "a"),
        (1, "b"),
        (2, "c"),
        (3, "a"),
        (4, "a"),
        (5, "c")
      )
    ).toDF("id","category")

    df.show()

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")

    val model = indexer.fit(df)
    val indexed = model.transform(df)

    indexed.show()

  }

    //sparkml中使用SparseVector来表示
    def testOneHotEncoding(): Unit = {
      val spark = SparkSession.builder().master("local").appName("StringIndexer").getOrCreate()

      //构造DF 一般都是喜欢使用Seq
      val df = spark.createDataFrame(
        Seq(
          (0, "a"),
          (1, "b"),
          (2, "c"),
          (3, "a"),
          (4, "a"),
          (5, "c")
        )
      ).toDF("id","category")

      df.show()

      val indexer = new StringIndexer()
        .setInputCol("category")
        .setOutputCol("categoryIndex")
        .fit(df)

      val indexed = indexer.transform(df)
      indexed.show()

      val encoder = new OneHotEncoder()
        .setInputCol("categoryIndex")
        .setOutputCol("categoryVec")

      val encoded = encoder.transform(indexed)
      encoded.show()

    }

  def main(args: Array[String]): Unit = {
//    testStringIndexer()
    testOneHotEncoding()
  }

}
