package com.scala

class SecondSortKey(var first:Int,val second:Int) extends Ordered[SecondSortKey] with Serializable {
  def compare(that:SecondSortKey):Int = {
    // - 这里什么意思？
    // scala 里面没有return  if里不是计算的语句是 return的值  >0 表示true
    if (this.first - that.first !=0){
      this.first -that.first
    }
    else {
      that.first - this.first
    }
  }
}
