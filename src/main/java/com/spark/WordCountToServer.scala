package com.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/*
  问题1:与hadoop存在类似的问题，output存在报错(空文件夹都是不允许存在的)。怎么写函数测试呢？


 */
object WordCountToServer {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: <file>")
      System.exit(1)
    }

    // 创建SparkConf 并设置App名称
    val conf = new SparkConf().setAppName("WC")
    //本机执行配置
    //val conf = new SparkConf().setAppName("wordCount").setMaster("local")

    //创建SparkContext，该对象是提交spark App的入口
    val sc = new SparkContext(conf)
    val line = sc.textFile(args(0))


    //本地演示，直接将统计结果输出到控制台 foreach(println)
    //line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println)

    //使用sc创建RDD并执行相应的transformation和action
    //将输出结果，按大小倒叙排序，保存到输出文件
//    line.flatMap(_.split("  ")).map((_, 1)).reduceByKey(_ + _, 1).sortBy(_._2, false).saveAsTextFile(args(1))
    line.flatMap(_.split("  ")).map((_, 1)).reduceByKey(_ + _, 1).sortBy(_._2, false).collect().foreach(println)

    sc.stop()
  }
}
