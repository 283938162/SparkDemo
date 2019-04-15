//package com.spark.kafka
//
//object ScalaKafkaStreaming {
//  def main(args: Array[String]): Unit = {
//
//    val conf = new SparkConf()
//      .setAppName("ScalaKafkaStream")
//      .setMaster("local[2]")
//
//    val sc = new SparkContext(conf)
//    sc.setLogLevel("WARN")
//
//    val ssc = new StreamingContext(sc, Seconds(5))
//    ssc.checkpoint(checkpointPath)
//
//    val bootstrapServers = "node1:9092,node2:9092:node3:9092"
//    val groupId = "kafka-test-group"
//    val topicName = "Test"
//    val maxPoll = 500
//
//    val kafkaParams = Map(
//      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
//      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
//      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> maxPoll.toString,
//      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
//      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
//    )
//
//    val kafkaTopicDS = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
//      ConsumerStrategies.Subscribe[String, String](Set(topicName), kafkaParams))
//
//    kafkaTopicDS.map(_.value)
//      .flatMap(_.split(" "))
//      .map(x => (x, 1L))
//      .reduceByKey(_ + _)
//      .transform(data => {
//        val sortData = data.sortBy(_._2, false)
//        sortData
//      })
//      .print()
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}