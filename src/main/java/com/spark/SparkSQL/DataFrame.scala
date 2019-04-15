package com.spark.SparkSQL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StringType


/**
  * 使用Spark SQL在对数据进行处理的过程中，
  * 可能会遇到对一列数据拆分为多列，或者把多列数据合并为一列。
  * 这里记录一下目前想到的对DataFrame列数据进行合并和拆分的几种方法。
  *
  * https://blog.csdn.net/lw_ghy/article/details/51480358
  */
object DataFrame {
  Logger.getLogger("DataFrame").setLevel(Level.WARN)

  //  def main(args: Array[String]): Unit = {
  val spark = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    .master("local")
    .getOrCreate()
  //
  //    //从内存中创建一组DataFrame数据
  //    import spark.implicits._
  //    // toDF参数可以为空
  //    val df = Seq(("Ming", 20, 15552211521L), ("hong", 19, 13287994007L), ("zhi", 21, 15552211523L)).toDF("name", "age", "phone")
  //外部数据  todo
  // https://blog.csdn.net/oxuzhenyi/article/details/72904788

  val emp = spark.read.json("input/department.json")
  val dep = spark.read.json("input/employee.json")


//  val df1 = Seq(("Ming", 20, 15552211521L), ("hong", 19, 13287994007L), ("zhi", 21, 15552211523L)).toDF("name", "age", "phone")
  //  val df2 = Seq(("Ming", 21, 12552311521L), ("zhang", 19, 19287994007L), ("li", 25, 15552211523L)).toDF("name", "age", "phone")
  // 以下两种都不行,必须使用Seq   Seq是列表，适合存有序重复数据，进行快速插入/删除元素等场景
  // val dfArray = Array(("Ming", 20, 15552211521L), ("hong", 19, 13287994007L), ("zhi", 21, 15552211523L)).toDF("name","age","phone")
  // val dfSet = Set(("Ming", 20, 15552211521L), ("hong", 19, 13287994007L), ("zhi", 21, 15552211523L)).toDF("name","age","phone")

  //    df.show()
  //    df.show(3,false)
  //    df.printSchema()
  //    df.select("name").show()
  //
  //    //列操作
  //    df.select(df("name"),df("age") + 1).show()
  //
  //    // 查询年龄大于19 且小于 21的
  //    df.filter(df("age")>19 && df("age") < 21)
  //
  //   // 排序
  //    df.sort(df("age").desc,df("phone").asc).show()
  //
  //    // 按年龄分组
  //    df.groupBy("age").count().show()
  //
  //    //对列进行重命名
  //    df.select(df("name").as("NAME")).show()


  // spark2.x 中 registerTempTable 已经改为 createOrReplaceTempView
  // 将DataFrame注册成一张名为stu的表：
  //    df.createOrReplaceTempView("stu")
  //    spark.sql("select * from stu").show()

  //
  //    df1.createOrReplaceTempView("stu1")
  //    spark.sql("select * from stu1").show()
  //
  //
  //    df2.createOrReplaceTempView("stu2")
  //    spark.sql("select * from stu2").show()

  // 内联：内联是默认的Join操作，它仅仅返回两个DataFrame都匹配到的结果
//  df1.join(df2, df1("name") === df2("name")).show()
//
//  // 右外联：在内连接的基础上，还包含右表中所有不符合条件的数据行，并在其中的左表列填写NULL
//  df1.join(df2, df1("name") === df2("name"), "right_outer").show()
//
//  // 左外联：在内连接的基础上，还包含左表中所有不符合条件的数据行，并在其中的右表列填写NULL
//  df1.join(df2, df1("name") === df2("name"), "left_outer").show()


}
