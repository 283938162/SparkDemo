package com.spark

import org.apache.spark.{SparkContext, SparkConf}
/*
spark 版本一定要和scala保持一致，不然代码报错
"main" java.lang.NoSuchMethodError: scala.Predef$.refArrayOps([Ljava/lang/Object;)Lscala/collection/mutable/ArrayOps;
 */
object WordCount {
  def main(args: Array[String]) {
    //设置本机Spark配置
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    //创建Spark上下
    val sc = new SparkContext(conf)
    //从文件中获取数据
    val input = sc.textFile("/Users/anus/IdeaProjects/SparkDemo/input/wordcount_input.txt")
    //分析并排序输出统计结果
    input.flatMap(line => line.split("  ")).map(word => (word, 1)).reduceByKey((x, y) => x + y).sortBy(_._2,false).foreach(println _)
  }
}
