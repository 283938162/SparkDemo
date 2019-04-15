package com.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/**** 进行远程提交，注意两点
- setMaster(master)：master变量必须为远程集群
- setJars(List(“D:\JetBrains\workspace\WordCount\out\artifacts\WordCount_jar\WordCount.jar”))：设置本地jar的目录
- 设置好后，点击运行即可
- 这种形式只是连接 连接到Spark standalone集群


-  异常 Caused by: java.lang.RuntimeException: java.io.StreamCorruptedException: invalid stream header: 01000D31
-  是不是因为本机是spark2.2  standlone是默认是1.6
-  未解决！
  */
object SparkRemoteSublimit {
  private val master = "spark://node1:7077"
  private val remote_file = "hdfs://node1:8020/user/tuoming/input/wordcount_input.txt"
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("SparkRemoteSublimit")
      .setMaster(master)
      .set("spark.executor.memory", "1G")
      .setJars(List("/Users/anus/IdeaProjects/SparkDemo/target/SparkTest-1.0-SNAPSHOT.jar"))

    val sc = new SparkContext(conf)
    val textFile = sc.textFile(remote_file)
    val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    wordCount.foreach(println)
  }
}
