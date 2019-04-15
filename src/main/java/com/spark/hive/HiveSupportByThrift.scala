package com.spark.hive

import org.apache.spark.sql.SparkSession

/*
  https://www.jianshu.com/p/983ecc768b55
  spark2 连接hive 跟spark1.6 还是存在区别


  操作集锦
  https://www.cnblogs.com/nucdy/p/6559318.html

  不依赖配置文件
 */
object HiveSupportByThrift {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("HiveSupport")
        .master("local[2]")
        .config("hive.metastore.uris", "thrift://node1:9083")//远程hive的meterstore地址
        .enableHiveSupport() //开启支持hive
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN") //设置日志输出级别
    //    import spark.implicits._
    //    import spark.sql

    spark.sql("show databases").show()

    spark.sql("use default")
    val stu = spark.sql("select * from student")
    stu.show()
    stu.printSchema()



    Thread.sleep(150 * 1000)



    spark.stop()
  }
}
