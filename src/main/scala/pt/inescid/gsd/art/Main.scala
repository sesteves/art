package pt.inescid.gsd.art

import argonaut._, Argonaut._

/**
 * Created by sesteves on 02-06-2015.
 */
object Main {


  def main(args: Array[String]): Unit = {

    val jsonStr = scala.io.Source.fromFile("sla").getLines.mkString
    val slas = jsonStr.decodeOption[List[SLA]].getOrElse(Nil)
    val sla = slas.find(_.application=="Ngrams").get
    println(slas)
    println(sla)

  }

}

