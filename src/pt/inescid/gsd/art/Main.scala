package pt.inescid.gsd.art

import argonaut._, Argonaut._

/**
 * Created by sesteves on 02-06-2015.
 */
object Main {


  def main(args: Array[String]): Unit = {

    val jsonStr = scala.io.Source.fromFile("sla").getLines.mkString
    val slas = jsonStr.decodeOption[List[SLA]].getOrElse(Nil)

    println(slas)

  }

}
case class SLA(application: String, maxExecTime: Double, accuracy: Option[Double], minAccuracy: Option[Double],
               cost: Option[Double], maxCost: Option[Double])

object SLA {
  implicit def SLACodecJson: CodecJson[SLA] =
    casecodec6(SLA.apply, SLA.unapply)("application", "maxExecTime", "accuracy", "minAccuracy", "cost", "maxCost")
}
