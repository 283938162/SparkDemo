import org.apache.spark.{SparkConf, SparkContext}

object TTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("ScalaGroupTop3")
      .setMaster("local[1]")

    val sc = new SparkContext(conf)

    sc.textFile("/Users/anus/IdeaProjects/SparkDemo/input/score_top3.txt")
      .map(line => {
        val datas = line.split(" ")
        (datas(0), datas(1))
      })
      .groupByKey()
      .map(group => (group._1, group._2.toList.sortWith(_ > _).take(3)))
      .sortByKey()
      .foreach(group => {
        println(group._1)
        group._2.foreach(println)
      })

    sc.stop()
  }
}
