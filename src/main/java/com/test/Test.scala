package com.test

import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]) {
    //设置本机Spark配置
    val conf = new SparkConf().setAppName("Test").setMaster("local")
    //创建Spark上下
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    //
    //    var arr = Array(("a","1"),("b","2"),("c","3"),("a","4"))
    //    val arrRdd = sc.parallelize(arr)
    //
    //    val arrRdd1 = arrRdd.reduceByKey((x,y) => x + y)
    //    arrRdd1.foreach(print)

    // groupbykey
    //    var regk = arrRdd.groupByKey()
    //
    //    //(2,CompactBuffer(234, 34))(1,CompactBuffer(123, 23))
    //    regk.foreach(print)
    //
    //    var re = regk.map(x => (x._1, x._2.size, x._2.reduceLeft(_ + _)))
    //
    //    // (2,2,23434)(1,2,12323)
    //    re.foreach(print)


    //    var str = "string"
    //    println(str.toUpperCase)
    //    val arr = Array("abc", "dfg")
    //
    //    var rdd = sc.parallelize(arr)
    //    rdd.map(x => {
    //      x.toUpperCase
    //    }).foreach(println)

    val cols = Array("id", "name", "age", "id")

    //    cols.toSet[String].foreach(print)

    sc.parallelize(cols).foreachPartition(record => {
      record.foreach(x => {
//        println(x)
        println("AA")
      })
      println("BB")
    })

    sc.stop()
  }
}
