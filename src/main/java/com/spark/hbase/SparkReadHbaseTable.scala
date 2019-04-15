package com.spark.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

object SparkReadHbaseTable {
  def main(args: Array[String]): Unit = {
    //声明一个hbase的配置对象
    val hbaseConf = HBaseConfiguration.create()
    //配置hbase的节点信息
    hbaseConf.set("hbase.zookeeper.quorum", "node1,node2,node3")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

    // 设置查询的表名
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "student")


    val sparkConf = new SparkConf();
    sparkConf.setAppName("SparkShowHbaseTable");
    sparkConf.setMaster("local")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")


    val stuRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = stuRDD.count()
    println("student RDD Count:" + count)


    stuRDD.foreach { case (_, result) => {
      //获取行键
      val key = Bytes.toString(result.getRow)
      //通过列族和列名获取列
      val age = Bytes.toString(result.getValue("info".getBytes, "age".getBytes))
      val name = Bytes.toString(result.getValue("info".getBytes, "name".getBytes))
      val sex = Bytes.toString(result.getValue("info".getBytes, "sex".getBytes))
      println("Row key:" + key + " Name:" + name + " Age:" + age + " sex:" + sex)
    }
    }
  }
}
