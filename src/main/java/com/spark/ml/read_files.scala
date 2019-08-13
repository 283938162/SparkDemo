package com.spark.ml

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


object read_files {

  def read_csv(): Unit = {
    val spark = SparkSession.builder().appName("fileRead").getOrCreate()
    import spark.implicits._
    val data1 = spark.read
      //          推断数据类型
      .option("inferSchema", true)
      //          设置空值
      .option("nullValue", "?")
      //          表示有表头，若没有则为false
      .option("header", true)
      //          文件路径
      .csv("input/test.csv")
      //          缓存
      .cache()
    //          打印数据格式
    data1.printSchema()
    //      显示数据,false参数为不要把数据截断
    data1.show(false)
  }


  def read_csv1(): Unit = {
    val spark = SparkSession.builder().appName("fileRead").getOrCreate()
    import spark.implicits._
    val data: DataFrame = spark.read.format("com.databricks.spark.csv")
      .option("header", "true") //在csv第一行有属性"true"，没有就是"false"
      .option("inferSchema", true.toString) //这是自动推断属性列的数据类型
      .load("input/test.csv")
    data.show()


  }

  //  spark2.x 之后 hiveSql  SparkSql的入口做了统一
  case class Info(A: String, B: String, C: String, D: String, E: String)

  def read_txt_to_df_by_case_class(): Unit = {
    val spark = SparkSession.builder().appName("fileRead").master("local").getOrCreate()
    import spark.implicits._
    val data: RDD[String] = spark.sparkContext.textFile("input/data.txt")

    // Create an RDD of Person objects 创建一个Info RDD对象 将 RDD 与 case class 关联
    val rdd = data.map {
      line => line.split(" ")
    }.map {
      line => Info(line(0).trim.toString(), line(1).trim.toString(), line(2).trim.toString(), line(3).trim.toString(), line(4).trim.toString())
    }



    val df = rdd.toDF()
    df.show()

    //  创建视图/注册成临时表
    //    df.registerTempTable()  spark1.x 创建临时表的api 已废弃
    df.createOrReplaceTempView("test")


    //子查询
    val s1 = spark.sql("select * from test where A = '22'")
    s1.show()

    //查询结果保存到文件
    df.select("A", "E").write.format("csv").save("input/test_out.csv")


  }


  def read_txt_to_df_by_structType(): Unit = {
    val spark = SparkSession.builder().master("local").appName("todf").getOrCreate()
    import spark.implicits._
    val fileRDD = spark.sparkContext.textFile("input/data.txt")
    // 将 RDD 数据映射成 Row，需要 import org.apache.spark.sql.Row
    // 空格" " 与 tab "\t"
    val rowRDD: RDD[Row] = fileRDD.map {
      line => {
        val fields = line.trim().split(' ')
        Row(fields(0).trim.toString(), fields(1).trim.toString(), fields(2).trim.toString(), fields(3).trim.toString(), fields(4).trim.toString())
      }
    }

    // 创建 StructType 来定义结构
    //    val structType: StructType = StructType(
    //      //字段名，字段类型，是否可以为空
    //      StructField("A", StringType, true) ::
    //        StructField("B", StringType, true) ::
    //        StructField("C", StringType, true) ::
    //        StructField("D", StringType, true) ::
    //        StructField("E", StringType, true) :: Nil
    //
    //    )

    // 两种结构效果一样
    val structType: StructType = StructType(
      Array(
        //字段名，字段类型，是否可以为空
        StructField("A", StringType, true),
        StructField("B", StringType, true),
        StructField("C", StringType, true),
        StructField("D", StringType, true),
        StructField("E", StringType, true)
      )
    )


    val df = spark.createDataFrame(rowRDD, structType)
    df.show()
  }

  def main(args: Array[String]): Unit = {


    //    read_csv1()

    //txt 创建DD的常见方式: case class反射 另一种是 structType 编程接口
    //        read_txt_to_df_by_case_class()
    read_txt_to_df_by_structType()
  }

}
