package com.spark

import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

/***
  * 本地spark
  * 远程mysql
  *
  */
object SparkVisitMysqlByJDBC{
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("mysql")
    .enableHiveSupport()
    .getOrCreate()

  def sparkReadMysql(): Unit = {
    val jdbcDF = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://node1:3306/tuoming")
      .option("driver","com.mysql.jdbc.Driver")
      .option("dbtable", "student")
      .option("user", "root")
      .option("password", "anus").load()
    jdbcDF.show()
  }

  def sparkWriteMysql(): Unit = {
    //下面我们设置两条数据表示两个学生信息
    val studentRDD = spark.sparkContext.parallelize(Array("30 Rongcheng 26","40 Guanhua 27")).map(_.split(" "))

    //下面要设置模式信息
    val schema = StructType(List(StructField("id", IntegerType, true),StructField("name", StringType, true),StructField("age", IntegerType, true)))

    //下面创建Row对象，每个Row对象都是rowRDD中的一行
    val rowRDD = studentRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).toInt))

    //建立起Row对象和模式之间的对应关系，也就是把数据和模式对应起来
    val studentDF = spark.createDataFrame(rowRDD, schema)

    //下面创建一个prop变量用来保存JDBC连接参数
    val prop = new Properties()
    prop.put("user", "root") //表示用户名是root
    prop.put("password", "anus") //表示密码是hadoop
    prop.put("driver","com.mysql.jdbc.Driver") //表示驱动程序是com.mysql.jdbc.Driver

    //下面就可以连接数据库，采用append模式，表示追加记录到数据库spark的student表中
    studentDF.write.mode("append").jdbc("jdbc:mysql://node1:3306/tuoming", "student", prop)
  }

  def main(args: Array[String]): Unit = {
//    sparkReadMysql()

      sparkWriteMysql()
      sparkReadMysql()
  }
}
