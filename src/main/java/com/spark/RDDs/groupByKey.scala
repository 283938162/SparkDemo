package com.spark.RDDs

import org.apache.spark.{SparkConf, SparkContext}

object groupByKey {
  def main(args: Array[String]) {
    //设置本机Spark配置
    val conf = new SparkConf().setAppName("Test").setMaster("local")
    //创建Spark上下
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    var arr = Array(("1","123"),("2","234"),("1","23"),("2","34"))
    val arrRdd = sc.parallelize(arr)

    // groupbykey
    var regk = arrRdd.groupByKey()

    //(2,CompactBuffer(234, 34))(1,CompactBuffer(123, 23))
    regk.foreach(print)

    // key  相同key出现的次数 相同key的值求和
    var re = regk.map(x => (x._1, x._2.size, x._2.reduceLeft(_ + _)))

    // (2,2,23434)(1,2,12323)
    re.foreach(print)

  }
}

