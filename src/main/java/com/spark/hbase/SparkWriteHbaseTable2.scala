package com.spark.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.spark._
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes


/*
   使用saveAsHadoopDataset写入数据
 */
object SparkWriteHbaseTable2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkSWriteHbaseTable2").setMaster("local")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val tablename = "student"

    sc.hadoopConfiguration.set("hbase.zookeeper.quorum","node1,node2,node3")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    val job = new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])


    val insertRDD = sc.makeRDD(Array("1005,29,zhaowu,male", "1006,15,wangqi,female"))
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

    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())

    sc.stop()
  }
}
