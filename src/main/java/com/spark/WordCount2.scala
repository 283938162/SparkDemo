package com.spark

import org.apache.spark.{SparkConf, SparkContext}

/*
spark 版本一定要和scala保持一致，不然代码报错
"main" java.lang.NoSuchMethodError: scala.Predef$.refArrayOps([Ljava/lang/Object;)Lscala/collection/mutable/ArrayOps;
 */
object WordCount2 {
  def main(args: Array[String]) {
    /**
      * SparkContext 的初始化需要一个SparkConf对象
      * SparkConf包含了Spark集群的配置的各种参数
      */
    val conf=new SparkConf() //conf是全局的 也是唯一的 所以这里使用var 定义一个全局不可变变量
      .setMaster("local")//启动本地化计算
      .setAppName("testRdd")//设置本程序名称

    //Spark程序的编写都是从SparkContext开始的
    val sc=new SparkContext(conf)  //本地测试 就不采用工厂模式了  直接new
    //以上的语句等价与val sc=new SparkContext("local","testRdd")
    val data=sc.textFile("/Users/anus/IdeaProjects/SparkDemo/input/wordcount_input.txt")//读取本地文件
    data.flatMap(_.split("  "))//下划线是占位符，flatMap是对行操作的方法，对读入的数据进行分割
      .map((_,1))//将每一项转换为key-value，数据是key，value是1
      .reduceByKey(_+_)//将具有相同key的项相加合并成一个
      .collect()//将分布式的RDD返回一个单机的scala array，在这个数组上运用scala的函数操作，并返回结果到驱动程序
      .foreach(println)//循环打印
  }
}
