package com.spark
import org.apache.spark.sql.SparkSession

object SparkVisitParqent {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("mysql")
    .enableHiveSupport()
    .getOrCreate()

  def sparkReadFromParquent(): Unit ={
    val parquetFileDF = spark.read.parquet("file:///Users/anus/IdeaProjects/SparkDemo/input/users.parquet")
    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = spark.sql("SELECT * FROM parquetFile")
    namesDF.foreach(attributes =>println("Name: " + attributes(0)+"  favorite color:"+attributes(1)))
  }

  def sparkWriteToParquent(): Unit = {
    val peopleDF = spark.read.json("file:///Users/anus/IdeaProjects/SparkDemo/input/spark-sql/people.json")
    // 无论Hadoop还是spark 写出的都是一个文件夹
    peopleDF.write.parquet("file:///Users/anus/IdeaProjects/SparkDemo/input/spark-sql/people.parquent")
  }

  def main(args: Array[String]): Unit = {

//     sparkReadFromParquent()
       sparkWriteToParquent()

  }
}
