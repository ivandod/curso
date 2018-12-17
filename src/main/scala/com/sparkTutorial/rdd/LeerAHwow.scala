package com.sparkTutorial.rdd

import java.sql.Date
import java.text.SimpleDateFormat

import scala.io.Source
import net.liftweb.json._
import net.liftweb.json.DefaultFormats
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType


object LeerAHwow {

  var url_datos:String = ""
  var fecha_json:String = ""

  def main(args: Array[String]) {

    implicit val formats = DefaultFormats

    //Obtenemos el JSON de la URL. De este modo se hace síncrono.
    val jValue = parse(Source.fromURL("https://eu.api.battle.net/wow/auction/data/dun-modr?locale=es_ES-locale&apikey=s8yyu2peeu3jetmjhf9typpds2jy6ukn").mkString)

    //Tipo de datos recibidos.
    case class DireccionWeb(url: String, lastModified: String)

    //Hijos de "files"
    val lista_files = (jValue \\ "files").children

    //Recorremos la lista
    for (datos <- lista_files) {

      //Creamos objetos de lo recibido
      val data = datos.extract[DireccionWeb]

      //Convertimos el lastModified a DD-MM-AAAA

      //Formato deseado
      val df = new SimpleDateFormat("dd-MM-yyyy")

      //Lo pasamos a Date de JAVA
      val daTe:Date = new Date(data.lastModified.toLong)

      //Pasamos el DATE a String con el formato deseado
      val java_date:String = df.format(daTe)

      //Ya lo tenemos
      println(data.lastModified +  " convertido a : " + java_date)

      println(data.url)
      fecha_json = java_date
      url_datos = data.url
    }

    
  }
}
