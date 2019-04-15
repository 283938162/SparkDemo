package com.spark.SparkSQL

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *  从0️开始构造DataFrame
  *
  *  0 -> rdd -> DF
  *
  *  进阶参考：
  *  /Users/anus/IdeaProjects/SparkDemo/src/main/java/com/spark/hive/HiveSupport.scala
  */
object CreateDF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkCore").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val checkData = Array(
      "2015,11,hadoop",
      "2016,11,spark",
      "2017,11,scala",
      "2018,22,java"
    )

    val consData = Array(
      "2015,11,java",
      "2016,11,scala",
      "2016,11,python",
      "2017,21,hadoop",
      "2018,22,hadoop"
    )

    val check_data_rdd = sc.parallelize(checkData)
    val cons_data_rdd = sc.parallelize(consData)

    // 拆分数据  一定要返回RDD[Row]对象
    val check_data_type = check_data_rdd.map(
      row => {
        val Array(cons_no, org_no, tag_code) = row.split(",")
        Row(cons_no, org_no, tag_code)
      }
    )

    //定义DataFrame的结构
    val structType = StructType(
      Array(
        StructField("cons_no",StringType,true),
        StructField("org_no",StringType,true),
        StructField("tag_code",StringType,true)
      )
    )


    val ss=SparkSession.builder().appName("SparkSql").master("local[2]").getOrCreate()
    val test_check_df = ss.createDataFrame(check_data_type,structType)

    test_check_df.show()


    //    以上可以更精简  直接使用Seq去构造你调用 toDF方法
    //    import spark.implicits._
    //    val df = Seq(("Ming", 20, 15552211521L), ("hong", 19, 13287994007L), ("zhi", 21, 15552211523L)).toDF("name","age","phone")
    //    df.show()


//    check_data_rdd.foreach(print)

  }
}
