package com.xxx.scalaL

import scala.Console.println
import scala.collection.mutable.ListBuffer

object Demo01 {
  def main(args: Array[String]): Unit = {

    //方法  函数
    //    方法是类的一部分，而函数是一个完整对象
    //    -->类中定义的函数即是方法
    // val 语句可以定义函数，def 语句定义方法。

    class Test01 {
      //    def functionName ([参数列表]) : [return type]
      //      没有返回值，可以返回为 Unit
      def m(x: Int) = x + 3

      val f = (x: Int) => x + 3

      println(m(333))
    }

    val t0 = new Test01
    println(t0.f(3))
    println(t0.m(4))


    ///
    //    var z:Array[String] = new Array[String](3)
    var z = Array("Runoob", "Baidu", "Google")

    val myMatrix = Array.ofDim[Int](3, 3)

    ////array固定大小
    ////List不可变 可重复
    val list = List(1, 2, 3)
    //   val lb= ListBuffer(1,2)
    //set  不重复  不（可变）
    // 默认引用 scala.collection.immutable（mutable）.Set
    import scala.collection.mutable.Set // 可以在任何地方引入 可变集合
    val set = Set(1, 2, 3)
    set.add(3)

    //    Map (k,v)   不（可变）
    val colors = Map("red" -> "#FF0000", "azure" -> "#F0FFFF")
    colors.keys.foreach { x =>
      print(x)
      println(colors(x))
    }

    //元组也是不可变的  含不同类型的元素
    val t = (1, 3.14, "haha") //new Tuple3(1,3.14,"haha")
    //    元组的实际类型取决于它的元素的类型 几个元素就tuple几 最大22
    println(t._2)
    //迭代元组
    t.productIterator.foreach(print(_)) //.foreach{ i =>println("Value = " + i )}

    //    5 Option Option[T] 是一个类型为 T 的可选值的容器： 如果值存在， Option[T] 就是一个 Some[T] ，如果不存在， Option[T] 就是对象 None 。
    val myMap: Map[String, String] = Map("key1" -> "value")
    val value1: Option[String] = myMap.get("key1")
    val value2: Option[String] = myMap.get("key2")

    println(value1) // Some("value1")
    println(value2) // None

    //Iterator（迭代器）
    val it = Iterator("Baidu", "Google", "Runoob", "Taobao")
    while (it.hasNext) {
      println(it.next())


    }


  }

}
