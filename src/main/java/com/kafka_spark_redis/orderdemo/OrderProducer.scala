package com.kafka_spark_redis.orderdemo

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.log4j.{Level, Logger}

import scala.util.Random




/**
  * Created by Administrator on 2017/8/30.
  * scala 生产者代码 https://blog.csdn.net/zhaoxiangchong/article/details/78379927
  * 将一条记录封装成 一个json 字符串发送到kafka
  */
object OrderProducer {
  //设置log的输出级别
  Logger.getLogger("OrderProducer").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {

    //Kafka参数设置
    val topic = "test"
    val brokers = "node1:9092,node2:9092:node3:9092"
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    val kafkaConfig = new ProducerConfig(props)
    //创建生产者
    val producer = new Producer[String, String](kafkaConfig)

    while (true) {
      //随机生成10以内ID
      val id = Random.nextInt(10)
      //创建订单事件
      val event = new JSONObject()
      event.put("id", id)
      event.put("price", Random.nextInt(10000))

      //发送信息
      producer.send(new KeyedMessage[String, String](topic, event.toString))
      println("Message sent: " + event)
      //随机暂停一段时间
      Thread.sleep(Random.nextInt(100))
    }
  }

}
