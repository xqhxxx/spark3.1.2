package com.xxx.scalaL


//类是对象的抽象，而对象是类的具体实例。类是抽象的，不占用内存，
// 而对象是具体的，占用存储空间。类是用于创建对象的蓝图，
// 它是一个定义包括在特定类型的对象中的方法和变量的软件模板。
class Point(xc: Int, yc: Int) {
  var x: Int = xc
  var y: Int = yc

  def move(dx: Int, dy: Int) {
    x = x + dx
    y = y + dy
    println("x 的坐标点: " + x);
    println("y 的坐标点: " + y);
  }
}

class Location(val xc: Int, val yc: Int,
               val zc: Int) extends Point(xc, yc) {
  var z: Int = zc

  def move(dx: Int, dy: Int, dz: Int) {
    x = x + dx
    y = y + dy
    z = z + dz
    println("x 的坐标点 : " + x);
    println("y 的坐标点 : " + y);
    println("z 的坐标点 : " + z);
  }
}

object Class_Object {
  def main(args: Array[String]): Unit = {

    //    val point = new Point(4, 5)
    //    point.move(1,1)


    //继承
    val loc = new Location(10, 20, 15);

    // 移到一个新的位置
    loc.move(10, 10, 5);

    //单例 伴生对象

    //    Trait(特征)  相当于java接口
    trait Equal {
      def isEqual(x: Any): Boolean
      def isNotEqual(x: Any): Boolean = !isEqual(x)

    }
  }

}
