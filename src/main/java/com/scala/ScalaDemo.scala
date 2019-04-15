package com.scala

import java.io.File
import java.util

import scala.io.Source

object ScalaDemo {
  def main(argsl: Array[String]): Unit = {
    //    val dirfile = new File("input/wprdcount")
    //    val files = dirfile.listFiles()
    //    for (file <- files) println(file)
    //
    //    var listFiles = files.toList
    //    //Map类型的可变数组
    //    val wordMap = scala.collection.mutable.Map[String, Int]()
    //
    //    listFiles.foreach(file => Source.fromFile(file).getLines().foreach(line => line.split(" ").foreach(word => {
    //      if (wordMap.contains(word)) {
    //        wordMap(word) += 1
    //      } else {
    //        wordMap += (word -> 1)
    //      }
    //    })))
    //    println(wordMap)
    //
    //    for ((key,value) <- wordMap ) println(key+":"+value)


    /*It`s show time*/

//    val list = List()
//    val listNew = "a"::list
////    val listNew = list::"b"   // 这种写法是错误的，将元素添加到列表  元素::List集合
//
//    val list1 = new util.ArrayList[String]()
//    list1.add("a")


    var arr= Array(("a",1),("b",2))
    println(arr.toMap)




  }
}
