package com.spark.kafka

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.SparkSession
/*
  spark读取kafka消息，并在控制台打印。
 */
object KafkaTest extends Serializable {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf();
    conf.setMaster("local")
    conf.setAppName("kafka")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val ssc =  new StreamingContext(sc, Seconds.apply(5))


    val sess = SparkSession.builder().config(conf).getOrCreate


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node1:9092,node2:9092:node3:9092",// kafka 集群
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "dsffaa",
      "auto.offset.reset" -> "earliest",  // 每次都是从头开始消费（from-beginning），可配置其他消费方式
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val topics = Array("flumeLogs")  //主题，可配置多个
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )



    val rd2=stream.map(e=>(e.value()))  //e.value() 是kafka消息内容，e.key为空值
    rd2.print()


    ssc.start()
    ssc.awaitTermination()

  }
}