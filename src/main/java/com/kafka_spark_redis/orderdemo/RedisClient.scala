package com.kafka_spark_redis.orderdemo

import redis.clients.jedis.JedisPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.{SparkConf, SparkContext}


object RedisClient extends Serializable {

  val redisHost = "192.168.221.101"
  val redisPort = 6379
  val redisTimeout = 30000
  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout, "123")

  lazy val hook = new Thread {
    override def run = {
//      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("test")
    val sc = new SparkContext(conf);
    sc.setLogLevel("WARN")

    val arr = Array(("a", 1), ("a", 2), ("b", 3), ("c", 4))
    val arrRdd = sc.parallelize(arr)


    val arrRdds = arrRdd.groupByKey().map(x => (x._1, x._2.size, x._2.reduceLeft(_ + _)))

    arrRdds.foreach(print)


    // MAP 是转换操作，懒加载 所以数据进不去

//    arrRdds.map{
//      x =>{
//        val dbIndex = 0
//        val jedis = RedisClient.pool.getResource
//        jedis.select(dbIndex)
//        jedis.set(x._1.toString, x._2.toString)
//
//        println(jedis.get(x._1.toString))
//        RedisClient.pool.returnResource(jedis)
//      }
//    }


    // foreach 是行动操作
    arrRdds.foreach{
      x =>{
            val dbIndex = 1
            val jedis = RedisClient.pool.getResource
            jedis.select(dbIndex)
            jedis.set(x._1.toString, x._2.toString)

            println(jedis.get(x._1.toString))
            RedisClient.pool.returnResource(jedis)
      }
    }




  }
}
