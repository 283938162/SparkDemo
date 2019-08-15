package com.spark.ml.官方学习案例

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StringType

object df_opts {

  def mergrColumns(): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    //从内存创建一组DataFrame数据
    import spark.implicits._
    val df = Seq(("Ming", 20, 15552211521L), ("hong", 19, 13287994007L), ("zhi", 21, 15552211523L))
      .toDF("name", "age", "phone")
    df.show()
    /**
      * +----+---+-----------+
      * |name|age|      phone|
      * +----+---+-----------+
      * |Ming| 20|15552211521|
      * |hong| 19|13287994007|
      * | zhi| 21|15552211523|
      * +----+---+-----------+
      */
    //方法1：利用map重写
    val separator = ","
    df.map(_.toSeq.foldLeft("")(_ + separator + _).substring(1)).show()

    /**
      * +-------------------+
      * |              value|
      * +-------------------+
      * |Ming,20,15552211521|
      * |hong,19,13287994007|
      * | zhi,21,15552211523|
      * +-------------------+
      */
    //方法2： 使用内置函数 concat_ws
    import org.apache.spark.sql.functions._
    df.select(concat_ws(separator, $"name", $"age", $"phone").cast(StringType).as("value")).show()

    /**
      * +-------------------+
      * |              value|
      * +-------------------+
      * |Ming,20,15552211521|
      * |hong,19,13287994007|
      * | zhi,21,15552211523|
      * +-------------------+
      */
    //方法3：使用自定义UDF函数

    // 编写udf函数
    def mergeCols(row: Row): String = {
      row.toSeq.foldLeft("")(_ + separator + _).substring(1)
    }

    val mergeColsUDF = udf(mergeCols _)
    df.select(mergeColsUDF(struct($"name", $"age", $"phone")).as("value")).show()

    /**
      * /**
      * * +-------------------+
      * * |              value|
      * * +-------------------+
      * * |Ming,20,15552211521|
      * * |hong,19,13287994007|
      * * | zhi,21,15552211523|
      * * +-------------------+
      **/
      */
  }

  def opts(): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    // 将json文件加载成一个dataframe
    val peopleDF = spark.read.json("C:\\Users\\Administrator\\IdeaProjects\\SparkSQLProject\\spark-warehouse\\people.json")
    // Prints the schema to the console in a nice tree format.
    peopleDF.printSchema

    // 输出数据集的前20条记录
    peopleDF.show

    //查询某列所有的数据： select name from table
    peopleDF.select("name").show

    // 查询某几列所有的数据，并对列进行计算： select name, age+10 as age2 from table
    peopleDF.select(peopleDF.col("name"), (peopleDF.col("age") + 10).as("age2")).show

    //根据某一列的值进行过滤： select * from table where age>19
    peopleDF.filter(peopleDF.col("age") > 19).show

    //根据某一列进行分组，然后再进行聚合操作： select age,count(1) from table group by age
    peopleDF.groupBy("age").count.show
  }

  def main(args: Array[String]): Unit = {
//    mergrColumns()
    opts()

  }
}
