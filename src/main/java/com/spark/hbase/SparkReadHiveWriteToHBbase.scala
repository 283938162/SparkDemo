package com.spark.hbase

import java.util

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession

/**
  * 1. 读取hive数据
  * 2. 批量写入hbase
  *
  *
  * 注意 Array List 是都是通过  obj(index) 小括号小标取数据的
  * https://www.cnblogs.com/simple-focus/p/6879971.html
  * Spark2.x SparkSession是总入口，包含了SqlContext 和 HiveContText
  *
  * 待测试  如果不适用foreachPartition直接使用foreach可不可行？ 可行的话效能如果？怎么评估？
  * 直接使用foreach会报错  一个connection refuse的错误。
  *
  * https://blog.csdn.net/Nougats/article/details/73550627
  */

object SparkReadHiveWriteToHBbase {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("HiveSupport")
        .master("local[2]") //本地测试开启这句
        .enableHiveSupport() //开启支持hive
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN") //设置日志输出级别
    spark.sql("use default")
    val stu = spark.sql("select * from student where name is not null")

    // 注册表之前都可以使用sql语句 为啥还是以注册？ 首先这个表是Temp 临时表 原始表经过各种连表操作之后已经没有对应的原始表
    // 如果还想使用结构化的sql语句去查询的话 就需要注册临时表
    // 还有就是sql读出的这个hive表 默认表名就是hive表名 已经在spark'sql中注册过了
    stu.createOrReplaceTempView("stu")


    //



    // foreachPartition  foreach
        stu.foreachPartition(record => {
          //声明一个hbase的配置对象
          val hbaseConf = HBaseConfiguration.create()
          //配置hbase的节点信息
          hbaseConf.set("hbase.zookeeper.quorum", "node1,node2,node3")
          hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

          val con = ConnectionFactory.createConnection(hbaseConf)
          val table = con.getTable(TableName.valueOf("stu"))

          //使用java的list集合  混合编程
          val list = new util.ArrayList[Put]()

          // 列名,放到一个数组里面，由于hbase插入数据要制定列名和值 插入时使用数组 取元素 arr(i)
          val cols = Array("id", "name", "age")

          record.foreach(row => {

            val rowkey = row.getAs[String]("id")

    //        spark 将每条需要写入hbase的记录封装成Put对象
            val put = new Put(Bytes.toBytes(rowkey))


            // spark 怎么遍历数组？  until关键字
            for (i: Int <- 0 until cols.length) {
              // Hbase 使用 getAs[T](列名)的方式获取对应单元格的值
              val value = row.getAs[Any](cols(i))
              if (value == null) {
                //Put.add方法接收三个参数：列族，列名，数据
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(cols(i)), Bytes.toBytes(""))
              } else {
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(cols(i)), Bytes.toBytes(value.toString))
              }
            }

            list.add(put)
          }
          )

          // 分区写入数据
          table.put(list)
          println("批量写入成功")

          //关闭连接
          table.close()

        }
          //forpartition end
        )


    // 不使用forrachpartionion

//    //声明一个hbase的配置对象
//    val hbaseConf = HBaseConfiguration.create()
//    //配置hbase的节点信息
//    hbaseConf.set("hbase.zookeeper.quorum", "node1,node2,node3")
//    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
//
//    val con = ConnectionFactory.createConnection(hbaseConf)
//    val table = con.getTable(TableName.valueOf("stu"))
//
//    //使用java的list集合  混合编程
//    val list = new util.ArrayList[Put]()
//
//    // 列名,放到一个数组里面，由于hbase插入数据要制定列名和值 插入时使用数组 取元素 arr(i)
//    val cols = Array("id", "name", "age")
//
//    stu.foreach(row => {   // 开始报错
//
//      val rowkey = row.getAs[String]("id")
//
//      //        spark 将每条需要写入hbase的记录封装成Put对象
//      val put = new Put(Bytes.toBytes(rowkey))
//
//
//      // spark 怎么遍历数组？  until关键字
//      for (i: Int <- 0 until cols.length) {
//        // Hbase 使用 getAs[T](列名)的方式获取对应单元格的值
//        val value = row.getAs[Any](cols(i))
//        if (value == null) {
//          //Put.add方法接收三个参数：列族，列名，数据
//          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(cols(i)), Bytes.toBytes(""))
//        } else {
//          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(cols(i)), Bytes.toBytes(value.toString))
//        }
//      }
//
//      list.add(put)
//    }
//    )
//
//    // 分区写入数据
//    table.put(list)
//    println("批量写入成功")
//
//    //关闭连接
//    table.close()
//
  }

}
