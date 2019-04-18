package com.test

import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}

object CommonUtils {

  // 判断删除文件路径
  def deletePath(sc: SparkContext, path: Path): Unit = {
    val hadoopConf = sc.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    if (hdfs.exists(path)) {
      //递归删除
      hdfs.delete(path, true)
    }
  }

  def isExists(sc: SparkContext, path: Path): Boolean = {
    val hadoopConf = sc.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    if (hdfs.exists(path)) {
      true
    } else {
      false
    }
  }


  def main(args: Array[String]): Unit = {
    //设置本机Spark配置
    val conf = new SparkConf().setAppName("Test").setMaster("local")
    //创建Spark上下
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val path = new Path("ml_model/lrmodel")

    deletePath(sc, path)
  }

}
