package com.spark.SparkSQL

import org.apache.spark.sql.SparkSession

/*
 *  https://blog.csdn.net/vfgbv/article/details/51578359
 *  spark2.x
 */
object DF_and_MYSQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    //第一种方法
    val jdbcDF = spark.read.format("jdbc")
      .option("url","jdbc:mysql://192.168.221.101:3306/cmcc")
      .option("dbtable","streaming_offset")
      .option("user","root").option("password","anus")
      .option("driver","com.mysql.jdbc.Driver")load()

    //第二种方法
    //val connectionProperties = new Properties()
    // connectionProperties.put("user","root")
    //connectionProperties.put("password","Liyijie331")
    // connectionProperties.put("driver","com.mysql.jdbc.Driver")
    //val jdbc2DF = spark.read.jdbc("jdbc:mysql://localhost:3306/streaming_offset","wordcount",connectionProperties)


//    jdbcDF.printSchema()
//    jdbcDF.show()

  }
}
