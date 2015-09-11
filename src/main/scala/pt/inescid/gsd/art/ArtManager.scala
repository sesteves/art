package pt.inescid.gsd.art

import java.io._
import java.net.{ServerSocket, InetSocketAddress}
import java.rmi.registry.LocateRegistry
import java.rmi.server.UnicastRemoteObject
import java.rmi.{RemoteException, Remote}
import java.util

import argonaut.Argonaut._
import argonaut.CodecJson
// import org.apache.spark.mllib.linalg.Vectors
// import org.apache.spark.mllib.regression.{LinearRegressionModel, LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.UnionRDD

import org.apache.spark.streaming.StreamingContext
import org.slf4j.Logger
import weka.classifiers.Classifier
import weka.classifiers.functions.{SimpleLinearRegression, LinearRegression}
import weka.core.{Instance, Instances, Attribute, DenseInstance}

import scala.collection.mutable.ArrayBuffer
import scala.io.{Source, BufferedSource}
import scala.concurrent.Lock


/**
 * Created by sesteves on 03-06-2015.
 */
class ArtManager(ssc: StreamingContext, sparkConf: SparkConf, setBatchDuration: Long => Unit)
  extends RemoteArtManager with Serializable {

  object Policies extends Enumeration {
    val MaximizeAccuracy = Value("maximize-accuracy")
    val MinimizeCost= Value("minimize-cost")
    val MinimizeTime = Value("minimize-time")
    val Balanced = Value("balanced")
  }

  import Policies._

  val DefaultPolicy = MinimizeCost

  val SLAFileName = "sla"
  val ProfileFileName = "profile"
  val ExecutorBootDuration = 6000
  val AccuracyChangeDuration = 6000
  val ArtServiceName = "artservice"
  val MaxAccuracy = 100
  val MinCost = 1
  val DefaultTotalExecutions = 2 * 60 / 10 * 6 + 6 // 2 minutes * 60 seconds / window of 10 seconds + remaining
  val DefaultReactWindowMultiple = 2
  val DefaultIdleDurationThreshold = 3000l
  val DefaultIdealDrift = 500l
  var DefaultJitterTolerance = 500l


  val appName = sparkConf.get("spark.app.name")
  val totalExecutions = sparkConf.getInt("spark.art.total.executions", DefaultTotalExecutions)
  val reactWindowMultiple = sparkConf.getInt("spark.art.react.window.multiple", DefaultReactWindowMultiple)
  val windowDuration = sparkConf.get("spark.art.window.duration").toLong
  val idleDurationThreshold = sparkConf.getLong("spark.art.idle.threshold", DefaultIdleDurationThreshold)
  val idealDrift = sparkConf.getLong("spark.art.ideal.drift", DefaultIdealDrift)
  val jitterTolerance = sparkConf.getLong("spark.art.jitter.tolerance", DefaultJitterTolerance)
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

// Consider to use this insteead of MLib: https://github.com/scalanlp/nak/blob/master/src/main/scala/nak/example/PpaExample.scala
// incremental/online learning http://moa.cms.waikato.ac.nz/details/classification/using-weka/

//  var model: LinearRegressionModel = null
//  val trainingSet = ArrayBuffer.empty[LabeledPoint]
  var seenMetrics = Set.empty[LearningMetrics]


  val AttributeNames = Array("accuracy", "ingestionRate", "cost", "windowDuration", "execTime")
  val attrs = new util.ArrayList[Attribute]
  AttributeNames.foreach(attr => attrs.add(new Attribute(attr)))
  val trainingInstances = new Instances("art", attrs, 0)
  trainingInstances.setClassIndex(0)
  val classifier = new SimpleLinearRegression
  // val classifierLinearRegression = new LinearRegression

  val lock = new Lock()
  var countUpdates = 0

  val policy = Policies.values.find(_.toString == sla.policy).getOrElse(DefaultPolicy)

  println(s"ART MANAGER ACTIVATED! (policy: $policy, idleDurationThreshold: $idleDurationThreshold)")
  println(s"ART file: $appName-$idleDurationThreshold-$accuracyStep-$jitterTolerance-$idealDrift-$windowDuration")
  println(s"ART metrics: timestamp,ingestionRate,accuracy,cost,window,delay,execTime")

  private var log : Logger = null

  var cost = 1
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

    // var trainingSet = ArrayBuffer.empty[LabeledPoint]

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

        //trainingSet += LabeledPoint(a, Vectors.dense(c, execTime))
      }
    }

    val diff = System.currentTimeMillis() - startTick
    println("ART profiling workload took " + diff + " ms")

    // Building the model
    //val numIterations = 100
    //val model = LinearRegressionWithSGD.train(ssc.sparkContext.makeRDD(trainingSet).cache(), numIterations)
    //model.save(ssc.sparkContext, s"$appName-model")
  }

  def profileWorkloadForBatchDuration: Unit = {

    println(s"ART profile:batchDuration,window,delay,time")

    (1000 to 10000).toStream.filter(10000 % _ == 0).foreach(bd => {
      setBatchDuration(bd)

      val trials = 4
      for (t <- 1 to trials) {
        lock.acquire()
        println(s"ART profile:$bd,$windowDuration,$delay,$execTime")
      }
    })
  }

  def increaseCost: Boolean = {
    if(cost < sla.maxCost.getOrElse(-1.0)) {
      // add resources
      // ssc.checkpoint()
      // println("ART STOPPING StreamingContext")
      // ssc.stop()

      cost += 1
      println(s"ART Increasing cost to $cost")
      ssc.sparkContext.requestExecutors(1)
      //delta = ExecutorBootDuration

      // println("ART STARTING StreamingContext")
      // ssc.start()
      return true
    }
    return false
  }

  def decreaseCost: Boolean = {
    if (sla.maxCost.isDefined && cost > MinCost) {
      cost -= 1
      println(s"ART Decreasing cost to $cost")
      ssc.sparkContext.killExecutors(null)
      return true
    }
    return false
  }

  def predictAccuracy: Boolean = {
    val ingestionRateMbps = (ingestionRate / 1000).toInt
    val targetExecTime = windowDuration - idealDrift
    if(seenMetrics.contains(LearningMetrics(ingestionRateMbps, cost, windowDuration, targetExecTime / 1000))) {

      println("ART This metric has already been seen!!!")
      //val predictedAccuracy = model.predict(Vectors.dense(ingestionRate, cost, windowDuration, targetExecTime))
      //accuracy = predictedAccuracy.toInt

      val instance = new DenseInstance(AttributeNames.length)
      instance.setDataset(trainingInstances)
      instance.setValue(0, 0.0)
      instance.setValue(1, ingestionRate.toDouble)
      instance.setValue(2, cost.toDouble)
      instance.setValue(3, windowDuration.toDouble)
      instance.setValue(4, targetExecTime.toDouble)
      val predictedAccuracy = classifier.classifyInstance(instance).toInt
      // val predictedAccuracyLR = classifierLinearRegression.classifyInstance(instance).toInt
      //println(s"ART RF: $predictAccuracy, LR: $predictedAccuracyLR")

      // consider checking if predicted accuracy respects sla specs

      accuracy = predictedAccuracy - (predictedAccuracy % accuracyStep)

      println(s"ART Predicted accuracy: $accuracy")
      return true
    }

    return false
  }

  def increaseAccuracy : Boolean = {
    if(accuracy < MaxAccuracy) {
      if(!predictAccuracy) {
        accuracy += accuracyStep
        println(s"ART Increasing accuracy to $accuracy ")
        // delta = delay + AccuracyChangeDuration
        // delta = windowDuration
      }
      return true
    }
    return false
  }

  def decreaseAccuracy: Boolean = {
    if (accuracy > sla.minAccuracy.getOrElse(MaxAccuracy)) {
      if(!predictAccuracy) {
        accuracy -= accuracyStep
        println(s"ART Decreasing accuracy to $accuracy")
        // delta = delay + AccuracyChangeDuration
        //  delta = windowDuration
      }
      return true
    }
    return false
  }

  def increaseBatchDuration: Boolean = {


    setBatchDuration(1000)


    return false
  }

  def decreaseBatchDuration: Boolean = {

    return false
  }

  def executeWorkload {
    while (true) {
      var delta = 0l
      countUpdates = 0
      println(s"ART Delay: $delay, ExecTime: $execTime")

      // if workload is not stable
      if (execTime > windowDuration + jitterTolerance) {
        println("ART ExecTime > WindowSize")

        policy match {
          case MaximizeAccuracy =>
            if(!increaseCost && !decreaseAccuracy) {
              println("ART Impossible trinity!")
            }

          case MinimizeCost =>
            if(!decreaseAccuracy && !increaseCost) {
              println("ART Impossible trinity!")
            }
          case Balanced =>
        }

      } else if (windowDuration - execTime > idleDurationThreshold) {

        policy match {
          case MaximizeAccuracy =>
            if (!increaseAccuracy && !decreaseCost) {
              println("ART Impossible trinity!")
            }
          case MinimizeCost =>
            if(!decreaseCost && !increaseAccuracy) {
              println("ART Impossible trinity!")
            }
          case Balanced =>
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

      profileWorkloadForBatchDuration
      System.exit(0)

      //executeWorkload

    }
  }.start()


  def markAsSeen {

    val ingestionRateMbps = (ingestionRate / 1000).toInt
    val execTimeSec = execTime / 1000
    if(seenMetrics.contains(LearningMetrics(ingestionRateMbps, cost, windowDuration, execTimeSec))) {
      return
    }

    // mark as seen if there is nothing else art can do or if we are already operating within ideal conditions
    if((accuracy == MaxAccuracy && cost == MinCost && execTime <= windowDuration) ||
      (execTime >= (windowDuration - idleDurationThreshold) && execTime <= windowDuration)) {
      println("ART Marking as seen")

      for (ir <- ingestionRateMbps - 2 to ingestionRateMbps + 2) {
        for (et <- execTimeSec - 2 to execTimeSec + 2) {
          seenMetrics += LearningMetrics(ir, cost, windowDuration, et)
        }
      }

      val newInstance = new DenseInstance(AttributeNames.length)
      newInstance.setDataset(trainingInstances)
      newInstance.setValue(0, accuracy.toDouble)
      newInstance.setValue(1, ingestionRate.toDouble)
      newInstance.setValue(2, cost.toDouble)
      newInstance.setValue(3, windowDuration.toDouble)
      newInstance.setValue(4, execTime.toDouble)

      trainingInstances.add(newInstance)
      classifier.buildClassifier(trainingInstances)
      // classifierLinearRegression.buildClassifier(trainingInstances)
    }
  }

  def updateExecutionTime(delay: Long, execTime: Long) {
    // println(s"ART updateExecutionTime: delay: $delay, execTime: $execTime")
    println(s"ART metrics: %d,$ingestionRate,$accuracy,$cost,$windowDuration,$delay,$execTime"
      .format(System.currentTimeMillis()))

    this.delay = delay
    this.execTime = execTime

    countUpdates += 1
    if(countUpdates == totalExecutions) {
      System.exit(0)
    }
    if (countUpdates % reactWindowMultiple == 0) {
      lock.release()
      markAsSeen
    }

    // online and incremental learning
//    trainingSet += LabeledPoint(accuracy, Vectors.dense(ingestionRate, cost, windowDuration, execTime))
//    val numIterations = 100
//    model = LinearRegressionWithSGD.train(ssc.sparkContext.makeRDD(trainingSet).cache(), numIterations)

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

case class LearningMetrics(ingestionRate: Int, cost: Int, windowDuration: Long, execTime: Long)

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
