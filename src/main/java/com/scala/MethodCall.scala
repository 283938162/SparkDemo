package com.scala
/*
 * scala 程序既可以调用自己的方法  也可以调用现成的java类中的方法
 */
object MethodCall {
  def main(args: Array[String]): Unit = {
    //Scala中方法的调用
    print(ScalaMethod.getSum(1, 2))

    print("\n")
    // Java中方法的调用
    print(JavaMethod.getSum(1, 2))
  }

}
