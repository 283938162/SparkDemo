package com.spark.SparkSQL

import java.util

/*
 *https://blog.csdn.net/high2011/article/details/52204573
 *
 * 怎么使用Seq  遍历往Seq里面写内容？ */
object SeqLearn {
  def main(args: Array[String]): Unit = {
    val s = Seq()
    println(s :+ ("a", "b") :+ ("c", "d"))

    val cols = List(1,2,3)
    println(cols)
    //用于添加和移除元素的操作符
    //操作符：coll:+elem 或 elem+:coll
    println(cols:+4)

    val list = new util.ArrayList()
    println(list)
  }
}