package com.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 分区原理
  * 分区方式  https://blog.csdn.net/zhangzeyuan56/article/details/80935034
  * 分区数跟打印的分区对不上?
  */
object SparkPartition {


  def main(args: Array[String]): Unit = {
    val logFile = "/Users/anus/IdeaProjects/SparkDemo/input/wordcount_input.txt" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    //文件路径 和 最小分区数
    //当指定分数区大于2时 下面的输出总是要比输入的大1
    val logData = sc.textFile(logFile, 2)

    //获取分区
    println("RDD分区数:" + logData.getNumPartitions)


    // repartition 从新分区
    val counts = sc.parallelize(List((1, "aa"), (2, "bb"), (3, "cc")), 3).repartition(3)
    println("RDD分区数1:" + logData.getNumPartitions)

    //    这种直接遍历是有问题的  non-empty iterator
    //    counts.foreachPartition(println)

    counts.foreachPartition(record => record.foreach(println))




    //    def mapPartitionsWithIndex[U](f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean = false)(implicit arg0: ClassTag[U]): RDD[U]
    //
    //    函数作用同mapPartitions，不过提供了分区的索引（代码中partid）。

    val rdd = sc.parallelize(List((1, "aa"), (2, "bb"), (3, "cc")), 3).repartition(4)

    /** repartition 之后数据都堆积在part3之后 这样很不好 数据倾斜
      * (part_0,List())
      * (part_1,List())
      * (part_2,List())
      * (part_3,List((1,aa), (2,bb), (3,cc)))
      */


    // 查看spark RDD 各分区内容
    getPartitionsWithIndex(logData)

  }


  // RDD[String]    RDD[(Any,Any)]
  def getPartitionsWithIndex(rdd: RDD[String]) = {
    // rdd自带的api遍历函数
    rdd.mapPartitionsWithIndex {
      (partid, iter) => {
        var part_map = scala.collection.mutable.Map[String, List[Any]]()
        var part_name = "part_" + partid
        part_map(part_name) = List[Any]()
        while (iter.hasNext) {
          part_map(part_name) :+= iter.next() //:+= 列表尾部追加元素
        }
        part_map.iterator
      }
    }.collect().foreach(println)
  }

}
