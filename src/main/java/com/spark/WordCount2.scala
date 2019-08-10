package com.spark

import org.apache.spark.{SparkConf, SparkContext}

/*
spark 版本一定要和scala保持一致，不然代码报错
"main" java.lang.NoSuchMethodError: scala.Predef$.refArrayOps([Ljava/lang/Object;)Lscala/collection/mutable/ArrayOps;

 （1）初始化集群配置对象
 （2）初始化SparkContext对象 这是spark程序的入门
 （3）创建RDD
 （4）调用各种算子进行迭代操作
 （5）保存/返回client数据

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
    data.flatMap(_.split(" "))//下划线是占位符，flatMap是对行操作的方法，对读入的数据进行分割
      .map((_,1))//将每一项转换为key-value，数据是key，value是1
      .reduceByKey(_+_)//将具有相同key的项相加合并成一个
      .sortBy(_._2,false)//等价于  .sortBy(x => x._2,false)  默认升序
      .collect()//将分布式的RDD返回一个单机的scala array，在这个数组上运用scala的函数操作，并返回结果到驱动程序
      .foreach(println)//循环打印
  }

//  一步直达  占位符  =>    x => x.xx()  这种可以使用占位符 直接  _.xx()
//  占位符简化编程操作   其实这里面的 line  word  x,y 本身没什么意思，都是临时变量 可以使用占位符替代
//  input.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((x, y) => x + y).sortBy(_._2,false).foreach(println _)

}
