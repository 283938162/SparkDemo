//package com.spark.kafka
//
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
//import org.apache.spark.streaming.kafka010.KafkaUtils
//import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//
//object SparkVisitKafka {
//  private  val brokers="node1:9092,node2:9092:node3:9092"
//  def main(args:Array[String]):Unit={
//    val spark=getSparkSession()
//    val sc=spark.sparkContext
//    val ssc=new StreamingContext(sc,Seconds(3))
//    val topics=Array("test")
//    val kafkaParams=Map[String,Object](
//      "bootstrap.servers"->brokers,
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer],
//      "group.id" -> "use_a_separate_group_id_for_each_stream",
//      "auto.offset.reset" -> "latest",
//      "enable.auto.commit" -> (false: java.lang.Boolean)
//    )
//    val messages=KafkaUtils.createDirectStream[String,String](
//      ssc,
//      PreferConsistent,
//      Subscribe[String,String](topics,kafkaParams)
//    )
//    val lines=messages.map(x=>{
//      x.value()
//    })
//    val wordCounts=lines.flatMap(x=>{
//      x.split(" ").map(x=>(x,1))
//    }).reduceByKey(_+_)
//    wordCounts.print()
//    ssc.start()
//    ssc.awaitTermination()
//  }
//
//  def getSparkSession():SparkSession={
//    val spark=SparkSession
//      .builder()
//      .appName("sparkSql")
//      .config("spark.some.config.option","some-value")
//      .master("local[4]")
//      .getOrCreate()
//    spark
//  }
//}
