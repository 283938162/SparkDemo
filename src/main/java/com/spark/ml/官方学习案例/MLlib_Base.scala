package com.spark.ml.官方学习案例
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.{Matrix, Matrices}

object MLlib_Base {
  def main(args: Array[String]): Unit = {
    val dv: Vector = Vectors.dense(2.0, 0.0, 8.0)
    print(dv)
    val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))
    print(sm)
  }
}
