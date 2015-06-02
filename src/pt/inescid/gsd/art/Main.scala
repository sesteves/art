package pt.inescid.gsd.art

import argonaut._, Argonaut._
import scalaz.\/

/**
 * Created by sesteves on 02-06-2015.
 */
object Main {


  def main(args: Array[String]): Unit = {

    val jsonStr = scala.io.Source.fromFile("sla").getLines.mkString

    // val result: String \/ Json = Parse.parse(jsonStr)


    val slas = jsonStr.decodeOption[List[SLA]].getOrElse(Nil)

    println(slas)


  }

  case class SLA(application: String, maxExecTime: Double, accuracy: Double, minAccuracy: Double,
                 cost: Double, maxCost: Double)

  object SLA {
    implicit def SLACodecJson: CodecJson[SLA] =
      casecodec6(SLA.apply, SLA.unapply)("application", "maxExecTime", "accuracy", "minAccuracy", "cost", "maxCost")
  }


}
