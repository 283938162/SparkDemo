package com.spark.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

/*
   使用saveAsHadoopDataset写入数据
   saveAsHadoopDataset用于将RDD保存到除了HDFS的其他存储中，比如HBase

   spark rdd批量写入Hbase
   https://www.cnblogs.com/yfb918/p/10471627.html
 */
object SparkWriteHbaseTable1 {
  def main(args: Array[String]): Unit = {
    //声明一个hbase的配置对象
    val hbaseConf = HBaseConfiguration.create()
    //配置hbase的节点信息
    hbaseConf.set("hbase.zookeeper.quorum", "node1,node2,node3")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

    // 设置查询的表名
    val tablename = "student"
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tablename)


    val sparkConf = new SparkConf();
    sparkConf.setAppName("SparkSWriteHbaseTable");
    sparkConf.setMaster("local")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val jobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)


    val insertRDD = sc.makeRDD(Array("1003,19,lili,male", "1004,25,zhangsan,female"))
    val rdd = insertRDD.map(_.split(',')).map { arr => {
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
      val put = new Put(Bytes.toBytes(arr(0).toString))  //rowkey
      put.add(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(arr(1)))
      put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(arr(2)))
      put.add(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes(arr(3)))
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, put)
    }
    }
    rdd.saveAsHadoopDataset(jobConf)

    sc.stop()
  }
}
