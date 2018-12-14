package rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, _}

object LeerCSV {

  var min:Long = 999
  var minWord = " "
  var maxWord = " "
  var max:Long = 0
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/PPINV_E_clientes_20180910_0049_s.txt")
    val words = lines.flatMap(line => line.split(";"))

    val wordCounts = words.countByValue()
    for ((word, count) <- wordCounts) {
      if (count < min) {
        printf(count + " es menor que " + min)
        minWord = word
        min = count
        }
      if (count > max) {
        maxWord = word
        max = count
      }
      println(word.trim() + " : " + count)

    }
    println("La palabra más usada: " + maxWord + " y sale " + max + " veces." )
    println("La palabra menos usada: " + minWord + " y sale " + min + " veces." )
  }
}
