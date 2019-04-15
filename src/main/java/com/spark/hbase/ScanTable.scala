package com.spark.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Scan}

object ScanTable {
  def main(args: Array[String]): Unit = {
    //声明一个hbase的配置对象
    val hbaseConf = HBaseConfiguration.create()
    //配置hbase的节点信息
    hbaseConf.set("hbase.zookeeper.quorum", "node1,node2,node3")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

    val con = ConnectionFactory.createConnection(hbaseConf)
    val table = con.getTable(TableName.valueOf("stu"))

    val scan = new Scan()
    val resultScanner = table.getScanner(scan)

    val iterator = resultScanner.iterator()
    while (iterator.hasNext) {
      val result = iterator.next()
      val cells = result.rawCells()

      println(cells)
    }

  }

}
