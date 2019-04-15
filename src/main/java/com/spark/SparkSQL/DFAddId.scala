package com.spark.SparkSQL

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * scala spark dataframe添加序号（id）列
  * *
  * https://blog.csdn.net/u013090676/article/details/80379371
  * 要学会看字段的数据类型  查看方法返回值  unit 返回值 一般就是空
  */
object DFAddId {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("HiveSupport")
        .master("local[2]")
        .enableHiveSupport() //开启支持hive
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN") //设置日志输出级别
    // 返回值是DF
    val stuDf = spark.sql("select name from student where name is not null")
    //这个返回值是 scala.Unit
    //    val stuDf1 = spark.sql("select name from student where name is not null").createOrReplaceTempView("stu")
    //    spark.sql("select * from stu").show()

    val stuRdd = stuDf.rdd
    stuRdd.foreach(println)
    /*
      [zhangsan]
      [lisi]
      [wangwu]
      [zhaoliu]
      [guijiaoqi]

     * 为啥DF 转 RDD 输出的是列表
     */

    // 利用输出结果每一行遍历的结果是list特性,
    // 默认索引从0开始，如果需要index 从1开始，使用map做个一个转换
    val newRdd = stuRdd.map(x => x(0)).zipWithIndex().map(x => (x._1, x._2.toInt + 1))

    // 利用DF中每一行的row特性
    //    val newRdd = stuRdd.map(row => row.getAs[String]("name")).zipWithIndex()

    // 字符串截取
    //    val newRdd = stuRdd.map(row => row.toString().substring(1, row.toString().length - 1)).zipWithIndex()
    newRdd.foreach(println)

    /**
      * RDD 转 DataFrame
      */

    // (1)  定义df结构
    val structType = StructType(
      Array(
        StructField("name", StringType, true),
        StructField("id", IntegerType, true)
      )
    )


    //(2) 构建RDD[Row]
    val rowRdd = newRdd.map(row => Row(row._1.toString, row._2.toInt))


    //(3)创建DF

    //第一个参数 是 RDD[Row]   RDD的类型是Row对象
    val newDf = spark.createDataFrame(rowRdd, structType)

    newDf.show()
  }

}
