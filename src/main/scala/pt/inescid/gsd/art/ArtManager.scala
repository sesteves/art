package pt.inescid.gsd.art

import java.rmi.{RemoteException, Remote}

import argonaut.Argonaut._
import argonaut.CodecJson
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.slf4j.Logger

/**
 * Created by sesteves on 03-06-2015.
 */
class ArtManager(ssc: StreamingContext, sparkConf: SparkConf) extends Runnable
  with RemoteArtManager with Serializable {

  val SLAFileName = "sla"
  val IdleDurationThreshold = 4000
  val ExecutorBootDuration = 6000

  val appName = sparkConf.get("spark.app.name")
  val windowDuration = sparkConf.get("spark.art.window.duration").toLong

  val jsonStr = scala.io.Source.fromFile(SLAFileName).getLines.mkString
  val slas = jsonStr.decodeOption[List[SLA]].getOrElse(Nil)
  val sla = slas.find(_.application == appName).get


  println("ART MANAGER ACTIVATED!")


  private var log : Logger = null

  var currentCost = 2
  @volatile var currentAccuracy = 1.0
  var delay: Long = -1
  var execTime: Long = -1

  println("ART windowDuration: " + windowDuration)


  def run() {
    var delta = 0

    // wait system to stabilize
    Thread.sleep(windowDuration * 4)

    while(true) {
      delta = 0
      println(s"ART Delay: $delay, ExecTime: $execTime")

      // if workload is not stable
      if(execTime > windowDuration) {
        println("ART ExecTime > WindowSize")


//        if(currentCost < sla.maxCost.getOrElse(-1.0)) {
//          // add resources
//          // ssc.checkpoint()
//          println("ART STOPPING StreamingContext")
//          // ssc.stop()
//
//          println("ART Requesting one more executor")
//          ssc.sparkContext.requestExecutors(1)
//
//          currentCost += 1
//          delta = ExecutorBootDuration
//
//          println("ART STARTING StreamingContext")
//          // ssc.start()
//        }

        if(currentAccuracy > sla.minAccuracy.getOrElse(1.0)) {


          currentAccuracy -= 0.1
          println("ART Decreasing Accuracy! currentAccuracy: " + currentAccuracy)


        }


      } else if(windowDuration - execTime > IdleDurationThreshold) {

        if(sla.maxCost.isDefined) {

          // ssc.sparkContext.killExecutor()

        }


      }


      Thread.sleep(windowDuration + delta)
    }
  }


  def updateExecutionTime(delay: Long, execTime: Long) {

    println(s"ART updateExecutionTime called!")

    this.delay = delay
    this.execTime = execTime
  }

  @throws(classOf[RemoteException])
  override def getAccuracy(): JsonNumber = currentAccuracy
}
case class SLA(application: String, maxExecTime: Double, accuracy: Option[Double], minAccuracy: Option[Double],
               cost: Option[Double], maxCost: Option[Double])

object SLA {
  implicit def SLACodecJson: CodecJson[SLA] =
    casecodec6(SLA.apply, SLA.unapply)("application", "maxExecTime", "accuracy", "minAccuracy", "cost", "maxCost")
}

trait RemoteArtManager extends Remote {
  @throws(classOf[RemoteException])
  def getAccuracy(): Double
}
