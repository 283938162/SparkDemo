package com.scala

/**
  * Scala 的函数
  * def 命名函数
  * getSum 函数名
  * a: Int,b:Int  参数名:参数类型  这一点跟java相反  Int可以省略 scala自动推送
  * Int 函数值的数据类型
  * = {方法体}
  * a + b  函数的结构  不需要return  以为上面已经使用了 =
  *
  *
  * 外部scala程序可以通过 Object名.方法名  实现调用。  类似java的静态方法
  */
object ScalaMethod {
  def getSum(a: Int,b:Int):Int = {
    a + b
  }
}
