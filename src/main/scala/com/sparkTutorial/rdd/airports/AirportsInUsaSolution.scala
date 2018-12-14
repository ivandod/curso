package com.sparkTutorial.rdd.airports

import java.io

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.io.Directory
import scala.reflect.io.File

object AirportsInUsaSolution {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    printf("Empezando JOB... \n")

    val conf = new SparkConf().setAppName("airports").setMaster("local[2]")
    val sc = new SparkContext(conf)
    printf("Borramos ficheros existentes... \n")
    val path = new Directory(new io.File("out/airports_in_usa.text"))
    path.deleteRecursively()

    val airports = sc.textFile("in/airports.text")
    val airportsInUSA = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(3) == "\"United States\"")

    val airportsNameAndCityNames = airportsInUSA.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER)
      splits(1) + ", " + splits(2)
    })
    airportsNameAndCityNames.saveAsTextFile("out/airports_in_usa.text")
    printf("Fichero guardado en OUT/")
  }
}
