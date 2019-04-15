package com.spark.hive

import org.apache.spark.sql.SparkSession

/*
  https://www.jianshu.com/p/983ecc768b55
  spark2 连接hive 跟spark1.6 还是存在区别


  操作集锦
  https://www.cnblogs.com/nucdy/p/6559318.html


  如果resource文件夹下不存在 hive-site.xml配置文件  也即是说这种连接hive方式依赖于配置文件
  *Exception in thread "main" org.apache.spark.sql.AnalysisException: Table or view not found: student; line 1 pos 14
 */
object HiveSupport {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("HiveSupport")
        .master("local[2]")
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


    // 没有注册表之前 是找不到 stu这个表的
    stu.createOrReplaceTempView("stu")


    // 根据已有列 来新增一列 并默认赋值为1.0  有意思
    spark.udf.register("addCol",(x:String)=> 1.0)
    spark.sql("select name,addCol(age) newCol from stu").show()


    // 获取所有的姓名 并添加序号







    //这个有问题  Caused by: java.lang.NullPointerException
//    spark.udf.register("toUpCase", (x: String) => {
//      x.toUpperCase
//    })
//
//    spark.udf.register("toAddAge", (x: Int) => (x + 2))
//
//    spark.udf.register("toList",(x:String)=> x.split(",").toSet[String].mkString(","))

//    spark.sql("select id,name,toList('1','2','3') from student").show()


      // drop  删除列

//    stu.filter(stu("age") >= 16).createOrReplaceTempView("stu")
//
//    spark.sql("select * from stu").show()
//
//    spark.sql("select * from student as a left join stu as b on a.id=b.id ").show()
//
//    spark.sql("select * from student as a left join stu as b on a.id=b.id where b.id is not null").show()
//
//    spark.sql("select id, concat_ws(',',name,age) as value from stu").show()


    Thread.sleep(150 * 1000)



    spark.stop()
  }
}
