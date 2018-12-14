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
    //Añadiendo (0) seleccionamos esa columna.
    val words = lines.map(line => line.split(";")(0))
    val NumRegistros = words.count()
    //Ahora podemos saber cuántos registros hay diferentes por columna TIPO_REG
    val wordCounts = words.countByValue()
    println(wordCounts + " registros.")
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
      println(word + " : " + count)

    }
    val porcentajeMax:Double = (100*max)/NumRegistros
    val porcentajeMin:Double = (100*min)/NumRegistros
    println("Los registros más comunes son: " + maxWord + " y sale " + max
        + " veces. El porcentaje de uso es: " + porcentajeMax + '%' )
    println("Los registros menos comunes son: " + minWord + " y sale " + min
      + " veces. El porcentaje de uso es: " + porcentajeMin + '%' )

  }
}
