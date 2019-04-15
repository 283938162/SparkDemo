//package com.kafka_spark_redis
//
//import kafka.serializer.StringDecoder
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming._
//import org.apache.spark.streaming.kafka.KafkaUtils
//import net.sf.json.JSONObject
//import redis.clients.jedis.JedisPool
//import org.apache.commons.pool2.impl.GenericObjectPoolConfig
//
//
//object UserClickCountAnalytics {
//  def main(args: Array[String]): Unit = {
//    var masterUrl = "local[1]"
//    if (args.length > 0) {
//      masterUrl = args(0)
//    }
//
//    // Create a StreamingContext with the given master URL
//    val conf = new SparkConf().setMaster(masterUrl).setAppName("UserClickCountStat")
//    val ssc = new StreamingContext(conf, Seconds(5))
//
//    // Kafka configurations
//    val topics = Set("user_events")
//    val brokers = "localhost:9092"
//    val kafkaParams = Map[String, String](
//      "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")
//    val dbIndex = 1
//    val clickHashKey = "app::users::click"
//
//    // Create a direct stream
//    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
//    val events = kafkaStream.flatMap(line => {
//      val data = JSONObject.fromObject(line._2)
//      Some(data)
//    })
//
//    // Compute user click times
//    val userClicks = events.map(x => (x.getString("uid"), x.getInt("click_count"))).reduceByKey(_ + _)
//    userClicks.foreachRDD(rdd => {
//      rdd.foreachPartition(partitionOfRecords => {
//        partitionOfRecords.foreach(pair => {
//          val uid = pair._1
//          val clickCount = pair._2
//          val jedis = RedisClient.pool.getResource
//          jedis.select(dbIndex)
//          jedis.hincrBy(clickHashKey, uid, clickCount)
//          RedisClient.pool.returnResource(jedis)
//        })
//      })
//    })
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
////复制代码
////上面代码使用了Jedis客户端来操作Redis，将分组计数结果数据累加写入Redis存储，如果其他系统需要实时获取该数据，直接从Redis实时读取即可。RedisClient实现代码如下所示：
////
////复制代码
////package com.sf.scalademo3
////
////import redis.clients.jedis.JedisPool
////import org.apache.commons.pool2.impl.GenericObjectPoolConfig
////
////object RedisClient extends Serializable {
////  val redisHost = "10.202.34.232"
////  val redisPort = 6383
////  val redisTimeout = 30000
////  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)
////
////  lazy val hook = new Thread {
////    override def run = {
////      println("Execute hook thread: " + this)
////      pool.destroy()
////    }
////  }
////  sys.addShutdownHook(hook.run)
////}