package com.spark.RDDs

import com.alibaba.fastjson.JSONObject
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object parseJson {
  def main(args: Array[String]) {
    //设置本机Spark配置
    val conf = new SparkConf().setAppName("Test").setMaster("local")
    //创建Spark上下
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")


    val id = Random.nextInt(10)
    //创建订单事件
    val event = new JSONObject()
    event.put("id", id)
    event.put("price", Random.nextInt(10000))

//    var arrJson = Array(event)
    var arrJson = Array(event)


    // 直接将json对象转rdd  如果是json串的话 还需要单独解析   参考 com.kafka_spark_redis.orderdemo.OrderConsumer
    val jsonRDD = sc.parallelize(arrJson)

    //{"price":7523,"id":5}
    jsonRDD.foreach(print)


    val orders = jsonRDD.map(x => (x.getString("id"), x.getLong("price"))).groupByKey().map(x => (x._1, x._2.size, x._2.reduceLeft(_ + _)))
    //(5,1,7523)
    orders.foreach(print)
  }
}

