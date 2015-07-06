package pt.inescid.gsd.art

import java.io._
import java.net.{ServerSocket, InetSocketAddress}
import java.rmi.registry.LocateRegistry
import java.rmi.server.UnicastRemoteObject
import java.rmi.{RemoteException, Remote}

import argonaut.Argonaut._
import argonaut.CodecJson
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.slf4j.Logger

import scala.io.BufferedSource

/**
 * Created by sesteves on 03-06-2015.
 */
class ArtManager(ssc: StreamingContext, sparkConf: SparkConf) extends RemoteArtManager with Serializable {

  val SLAFileName = "sla"
  val IdleDurationThreshold = 4000
  val ExecutorBootDuration = 6000
  val AccuracyChangeDuration = 6000
  val ArtServiceName = "artservice"

  val appName = sparkConf.get("spark.app.name")
  val windowDuration = sparkConf.get("spark.art.window.duration").toLong

  val jsonStr = scala.io.Source.fromFile(SLAFileName).getLines.mkString
  val slas = jsonStr.decodeOption[List[SLA]].getOrElse(Nil)
  val sla = slas.find(_.application == appName).get

  println("ART MANAGER ACTIVATED!")

  private var log : Logger = null

  var cost = 2
  @volatile var accuracy = 100
  var delay: Long = -1
  var execTime: Long = -1

  println("ART windowDuration: " + windowDuration)

  //System.setProperty("java.rmi.server.hostname", "localhost")
//  val stub = UnicastRemoteObject.exportObject(this, 0).asInstanceOf[RemoteArtManager]
//  val registry = LocateRegistry.getRegistry
//  registry.rebind(ArtServiceName, stub)



//  val address  = new InetSocketAddress("localhost", 8080)
//  val service = new FactorialServer0
//  val env = service.environment
//  val startServer = env.serve(address)
//  val shutdown = startServer.run


  def profileWorkload: Unit = {

    println(s"ART profile:executors,accuracy,window,delay,time")
    val startTick = System.currentTimeMillis()
    for(c <- 1.0 to sla.maxCost.getOrElse(-1.0) by 1.0) {
      if(c > 1.0) {
        ssc.sparkContext.requestExecutors(1)
      }

      for(a <- sla.minAccuracy.getOrElse(100) to 100 by 10) {
        // stop accumulating blocks on the queue
        accuracy = 0
        Thread.sleep(delay)

        accuracy = a
        // ssc.start()
        Thread.sleep(execTime + AccuracyChangeDuration)

        for(i <- 1 to 5) {
          println(s"ART profile:$c,$a,$windowDuration,$delay,$execTime")
          Thread.sleep(execTime + 1000)
        }
        // ssc.stop()
      }
    }

    val diff = System.currentTimeMillis() - startTick
    println("ART profiling workload took " + diff + " ms")

  }

  def executeWorkload {
    while (true) {
      var delta = 0
      println(s"ART Delay: $delay, ExecTime: $execTime")

      // if workload is not stable
      if (execTime > windowDuration) {
        println("ART ExecTime > WindowSize")


        //        if(cost < sla.maxCost.getOrElse(-1.0)) {
        //          // add resources
        //          // ssc.checkpoint()
        //          println("ART STOPPING StreamingContext")
        //          // ssc.stop()
        //
        //          println("ART Requesting one more executor")
        //          ssc.sparkContext.requestExecutors(1)
        //
        //          cost += 1
        //          delta = ExecutorBootDuration
        //
        //          println("ART STARTING StreamingContext")
        //          // ssc.start()
        //        }

        if (accuracy > sla.minAccuracy.getOrElse(100)) {


          accuracy -= 5
          println("ART Decreasing *Accuracy! currentAccuracy: " + accuracy)
          delta = AccuracyChangeDuration

        }


      } else if (windowDuration - execTime > IdleDurationThreshold) {

        if (sla.maxCost.isDefined) {

          // ssc.sparkContext.killExecutor()

        }


      }


      Thread.sleep(windowDuration + delta)
    }
  }

  new Thread {
    override def run() {
      // wait system to stabilize
      Thread.sleep(windowDuration * 4)

      println(s"ART Delay: $delay, ExecTime: $execTime")
      profileWorkload
      System.exit(0)

    }
  }.start()


  def updateExecutionTime(delay: Long, execTime: Long) {
    println(s"ART updateExecutionTime called!")

    this.delay = delay
    this.execTime = execTime
  }

  @throws(classOf[RemoteException])
  override def getAccuracy(): JsonNumber = accuracy


  new Thread {
    override def run() : Unit = {

      val server = new ServerSocket(9999)
      while (true) {
        val s = server.accept()
        val in = new BufferedSource(s.getInputStream()).getLines()
        val out = new PrintStream(s.getOutputStream())

        out.println(accuracy)
        out.flush()
        s.close()
      }

    }
  }.start()

}

case class SLA(application: String, maxExecTime: Double, accuracy: Option[Int], minAccuracy: Option[Int],
               cost: Option[Double], maxCost: Option[Double], policy: Option[String])

object SLA {
  implicit def SLACodecJson: CodecJson[SLA] =
    casecodec7(SLA.apply, SLA.unapply)("application", "maxExecTime", "accuracy", "minAccuracy",
      "cost", "maxCost", "policy")
}

trait RemoteArtManager extends Remote {
  @throws(classOf[RemoteException])
  def getAccuracy(): Double
}
