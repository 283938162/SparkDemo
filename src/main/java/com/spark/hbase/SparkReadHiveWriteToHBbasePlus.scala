package com.spark.hbase

import java.util

import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.SparkSession

/**
  * 1. 读取hive数据
  * 2. 批量写入hbase
  *
  *
  * 注意 Array List 是都是通过  obj(index) 小括号小标取数据的
  * https://www.cnblogs.com/simple-focus/p/6879971.html
  * Spark2.x SparkSession是总入口，包含了SqlContext 和 HiveContText
  *
  * 待测试  如果不适用foreachPartition直接使用foreach可不可行？ 可行的话效能如果？怎么评估？
  * 直接使用foreach会报错  一个connection refuse的错误。
  *
  * https://blog.csdn.net/Nougats/article/details/73550627
  */

object SparkReadHiveWriteToHBbasePlus {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("HiveSupport")
        .master("local[2]") //本地测试开启这句
        .enableHiveSupport() //开启支持hive
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN") //设置日志输出级别
    spark.sql("use default")
    val stu = spark.sql("select * from student where name is not null")

    // 注册表之前都可以使用sql语句 为啥还是以注册？ 首先这个表是Temp 临时表 原始表经过各种连表操作之后已经没有对应的原始表
    // 如果还想使用结构化的sql语句去查询的话 就需要注册临时表
    // 还有就是sql读出的这个hive表 默认表名就是hive表名 已经在spark'sql中注册过了
    stu.createOrReplaceTempView("stu")


  }

}
