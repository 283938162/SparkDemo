package com.spark.redis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/*
  sparkStraming 读取本地文件 跟spark读取不一样的
  https://blog.csdn.net/haohaixingyun/article/details/65449714
 */
object RedisTest {
  //设置log的输出级别
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    //设置本机Spark配置
    val conf = new SparkConf().setAppName("RedisTest").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(15))


    //监控的是文件夹！！！ 不是spark core的某个文件
    //val inputDS = ssc.textFileStream("/Users/anus/IdeaProjects/SparkDemo/input/wordcount_input.txt")
    val inputDS = ssc.textFileStream("/Users/anus/IdeaProjects/SparkDemo/input/dsfiles/")



    val words = inputDS.flatMap(_.split(" "))
    val wordAndOne  = words.map((_,1))
    val wordCount  = wordAndOne.reduceByKey(_+_)

    wordCount.print()


    //将结果写出到redis。固定范式 将ds写入redis
    wordCount.foreachRDD({
      rdd =>
        rdd.foreachPartition({
          it =>
            //建立redis的客户端连接
            val jedis = JedisConnectionPool.getConnections()
            //然后将一个区里面的数据一条一条的写出
            it.foreach({
              wordCount =>
                //清楚结果类型
                jedis.incrBy(wordCount._1,wordCount._2)
                print("插入redis成功!")
            })
            jedis.close()
        })
    })
    ssc.start()
    ssc.awaitTermination()

  }
}
