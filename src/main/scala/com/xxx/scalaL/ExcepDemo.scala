package com.xxx.scalaL

import java.io.{FileNotFoundException, FileReader, IOException}

object ExcepDemo {
  def main(args: Array[String]): Unit = {

//    在 Scala 里，借用了模式匹配的思想来做异常的匹配
    try {
      val f = new FileReader("input.txt")
    } catch {
      case ex: FileNotFoundException =>{
        println("Missing file exception")
      }
      case ex: IOException => {
        println("IO Exception")
      }
    } finally {
      println("Exiting finally...")
    }
  }

}
