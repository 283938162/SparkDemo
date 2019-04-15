package com.spark
import org.apache.spark.sql.SparkSession

/***
  * spark2-submit  --class com.spark.SparkVisitHive --master yarn-client  --executor-memory 500M /mnt/hgfs/anus/IdeaProjects/SparkDemo/target/SparkTest-1.0-SNAPSHOT.jar
  *
  */
object SparkVisitHive {
  case class Record(key: Int, value: String)
  def sparkVistiLocalHive(): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("hive")
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

    sql("show databases").show()
  }

  def main(args: Array[String]): Unit = {
//    sparkVistiLocalHive()
//    val conf = new SparkConf()
//    conf.setAppName("WordCount").setMaster("local")
//    val hive = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
//    hive.sql("show databases").show()

    val warehouseLocation = "spark-warehouse"
    val spark = SparkSession.builder().appName("Spark Hive Example").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()
    import spark.sql
    sql("SELECT * FROM sparktest.student").show()
  }


}
