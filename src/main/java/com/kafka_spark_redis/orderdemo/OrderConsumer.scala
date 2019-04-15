package com.kafka_spark_redis.orderdemo

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
  * Created by Administrator on 2017/8/30.
  */
object OrderConsumer {
  //设置日志输出级别
  Logger.getLogger("OrderConsumer").setLevel(Level.WARN)


  //Redis配置
  val dbIndex = 0
  //每件商品总销售额
  val orderTotalKey = "app::order::total"
  //每件商品每分钟销售额
  val oneMinTotalKey = "app::order::product"
  //总销售额
  val totalKey = "app::order::all"


  def main(args: Array[String]): Unit = {

    // 创建 StreamingContext 时间片为1秒
    val conf = new SparkConf().setMaster("local[2]").setAppName("OrderConsumer")
    val ssc = new StreamingContext(conf, Seconds.apply(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node1:9092,node2:9092:node3:9092", // kafka 集群
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "dsffaa",
      "auto.offset.reset" -> "earliest", // 每次都是从头开始消费（from-beginning），可配置其他消费方式
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val topics = Array("test") //主题，可配置多个
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val lines = kafkaStream.map(x => {x.value()})
    lines.print()


    print("lines:"+lines)
    //解析JSON 旧api
    //    val events = kafkaStream.flatMap(line => Some(JSON.parseObject(line._2))

    //解析JSON  将string转换成json对象  新api
    //Option有两个子类别，Some和None。当程序回传Some的时候，代表这个函式成功地给了你一个String，而你可以透过get()函数拿到那个String，如果程序返回的是None，则代表没有字符串可以给你
    val events = kafkaStream.flatMap(line => Some(JSON.parseObject(line.value())))
    print("events:" + events)

    // 按ID分组统计个数与价格总合
    val orders = events.map(x => (x.getString("id"), x.getLong("price"))).groupByKey().map(x => (x._1, x._2.size, x._2.reduceLeft(_ + _)))
    print("orders:" + orders)



    //输出
    orders.foreachRDD(x =>
      x.foreachPartition(partition =>
        partition.foreach(x => {

          println("id=" + x._1 + " count=" + x._2 + " price=" + x._3)

          //保存到Redis中
          val jedis = RedisClient.pool.getResource
          //Redis Select 命令用于切换到指定的数据库，数据库索引号 index 用数字值指定，以 0 作为起始索引值。
          jedis.select(dbIndex)
          //每个商品销售额累加
          jedis.hincrBy(orderTotalKey, x._1, x._3)
          //上一分钟第每个商品销售额
          jedis.hset(oneMinTotalKey, x._1.toString, x._3.toString)
          //总销售额累加
          jedis.incrBy(totalKey, x._3)
          RedisClient.pool.returnResource(jedis)

        })
      ))


    ssc.start()
    ssc.awaitTermination()
  }

}

