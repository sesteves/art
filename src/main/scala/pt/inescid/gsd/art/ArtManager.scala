package pt.inescid.gsd.art

import java.io._
import java.net.{ServerSocket, InetSocketAddress}
import java.rmi.registry.LocateRegistry
import java.rmi.server.UnicastRemoteObject
import java.rmi.{RemoteException, Remote}

import argonaut.Argonaut._
import argonaut.CodecJson
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.UnionRDD

import org.apache.spark.streaming.StreamingContext
import org.slf4j.Logger

import scala.collection.mutable.ArrayBuffer
import scala.io.{Source, BufferedSource}
import scala.concurrent.Lock

/**
 * Created by sesteves on 03-06-2015.
 */
class ArtManager(ssc: StreamingContext, sparkConf: SparkConf) extends RemoteArtManager with Serializable {

  val SLAFileName = "sla"
  val ProfileFileName = "profile"
  val ExecutorBootDuration = 6000
  val AccuracyChangeDuration = 6000
  val ArtServiceName = "artservice"
  val MaxAccuracy = 100

  val appName = sparkConf.get("spark.app.name")
  val windowDuration = sparkConf.get("spark.art.window.duration").toLong
  val idleDurationThreshold = sparkConf.get("spark.art.idle.threshold", "3000").toLong
  val accuracyStep = sparkConf.get("spark.art.accuracy.step", "10").toInt

  // loading sla
  val jsonStr = scala.io.Source.fromFile(SLAFileName).getLines.mkString
  val slas = jsonStr.decodeOption[List[SLA]].getOrElse(Nil)
  val sla = slas.find(_.application == appName).get


  // loading profile
//  var trainingSet = ArrayBuffer.empty[LabeledPoint]
//
//  for(line <- Source.fromFile(ProfileFileName).getLines()) {
//    val items = line.split(",")
//    trainingSet += LabeledPoint(items(0).toDouble, Vectors.dense(items(1).toDouble,items(2).toDouble))
//  }
//
//  val numIterations = 100
//  val model = LinearRegressionWithSGD.train(ssc.sparkContext.makeRDD(trainingSet).cache(), numIterations)


  val lock = new Lock()
  var countUpdates = 0

  println(s"ART MANAGER ACTIVATED! (idleDurationThreshold: $idleDurationThreshold)")
  println(s"ART metrics: timestamp,ingestionRate,accuracy,cost,window,delay,execTime")

  private var log : Logger = null

  var cost = 2
  @volatile var accuracy = 100
  var delay: Long = -1
  var execTime: Long = -1
  var ingestionRate: Long = -1

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

    var trainingSet = ArrayBuffer.empty[LabeledPoint]

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
        Thread.sleep(delay + windowDuration)

        var sumExecTime = 0l
        val trials = 10
        for(i <- 1 to trials) {
          lock.acquire()
          println(s"ART profile:$c,$a,$windowDuration,$delay,$execTime")
          sumExecTime += execTime
        }
        // ssc.stop()

        // average and add to the training set
        val avgExecTime = sumExecTime / trials
        println(s"ART profile2:$c,$a,$windowDuration,$delay,$avgExecTime")

        trainingSet += LabeledPoint(a, Vectors.dense(c, execTime))
      }
    }

    val diff = System.currentTimeMillis() - startTick
    println("ART profiling workload took " + diff + " ms")

    // Building the model
    val numIterations = 100
    val model = LinearRegressionWithSGD.train(ssc.sparkContext.makeRDD(trainingSet).cache(), numIterations)
    model.save(ssc.sparkContext, s"$appName-model")

  }

  def executeWorkload {
    while (true) {
      var delta = 0l
      countUpdates = 0
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


          accuracy -= accuracyStep
          println("ART Decreasing Accuracy! currentAccuracy: " + accuracy)
          // delta = delay + AccuracyChangeDuration
          delta = windowDuration
        }


      } else if (windowDuration - execTime > idleDurationThreshold) {

        if (sla.maxCost.isDefined) {
          // ssc.sparkContext.killExecutor()
        }

        if(accuracy < MaxAccuracy) {
          accuracy += accuracyStep
          println("ART Increasing Accuracy! currentAccuracy: " + accuracy)
          // delta = delay + AccuracyChangeDuration
          delta = windowDuration
        }

      }

      // Thread.sleep(windowDuration + delta)
      lock.acquire()
    }
  }

  new Thread {
    override def run() {
      // wait system to stabilize
      Thread.sleep(windowDuration * 4)

      println(s"ART Delay: $delay, ExecTime: $execTime")

      // profileWorkload
      // System.exit(0)

      executeWorkload

    }
  }.start()


  def updateExecutionTime(delay: Long, execTime: Long) {
    // println(s"ART updateExecutionTime: delay: $delay, execTime: $execTime")
    println(s"ART metrics: %d,$ingestionRate,$accuracy,$cost,$windowDuration,$delay,$execTime"
      .format(System.currentTimeMillis()))

    this.delay = delay
    this.execTime = execTime

    countUpdates += 1
    if (countUpdates == 2) {
      lock.release()
    }
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
        ingestionRate = in.next().toLong
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
