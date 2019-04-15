package com.spark.redis

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/*
https://blog.csdn.net/weixin_43345864/article/details/84310983
 */

object  JedisConnectionPool {
  //连接配置
  val config= new JedisPoolConfig
  //最大连接数
  config.setMaxTotal(20)
  //最大空闲连接数
  config.setMaxIdle(10)
  //设置连接池属性分别有： 配置  主机名   端口号  连接超时时间    Redis密码
//  val pool=new JedisPool(config,"192.168.221.101",6379,10000,"123")
  val pool=new JedisPool(config,"192.168.221.101",6379,10000,"123")
  //连接池
  def getConnections(): Jedis ={
    pool.getResource
  }
}