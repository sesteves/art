package pt.inescid.gsd.art

import java.io._
import java.net.{ServerSocket, InetSocketAddress}
import java.rmi.{RemoteException, Remote}
import java.util

import argonaut.Argonaut._
import argonaut.CodecJson
// import org.apache.spark.mllib.linalg.Vectors
// import org.apache.spark.mllib.regression.{LinearRegressionModel, LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.SparkConf

import org.apache.spark.streaming.{Seconds, Duration, StreamingContext}
import org.slf4j.Logger
import weka.classifiers.functions.{SimpleLinearRegression, LinearRegression}
import weka.core.{Instance, Instances, Attribute, DenseInstance}

import scala.io.{Source, BufferedSource}
import scala.concurrent.Lock


/**
 * Created by sesteves on 03-06-2015.
 */
class ArtManager(ssc: StreamingContext, sparkConf: SparkConf, setBatchDuration: Long => Unit,
                 setWindowDuration: (Duration) => Unit)
  extends RemoteArtManager with Serializable {

  object Modes extends Enumeration {
    val Profile = Value("profile")
    val Execute = Value("execute")
  }
  import Modes._

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
  val DefaultMode = Execute
  val DefaultTotalExecutions = 2 * 60 / 10 * 6 + 6 // 2 minutes * 60 seconds / window of 10 seconds + remaining
  val DefaultReactWindowMultiple = 2
  // val DefaultIdleDurationThreshold = 3000l
  val DefaultIdleDurationThreshold = 0.2
  val DefaultIdealDrift = 500l
  val DefaultJitterTolerance = 0.05
  val DefaultPredict = true
  val DefaultAccuracyStep = 0.001
  val DefaultCostStep = 1
  val DefaultIngestionRateScaleFactor = 1000
  val DefaultSpikeThreshold = 0.2


  val appName = sparkConf.get("spark.app.name")
  val mode = Modes.values.find(_.toString == sparkConf.get("spark.art.mode", DefaultMode.toString))
    .getOrElse(DefaultMode)
  val totalExecutions = sparkConf.getInt("spark.art.total.executions", DefaultTotalExecutions)
  val reactWindowMultiple = sparkConf.getInt("spark.art.react.window.multiple", DefaultReactWindowMultiple)
  val windowDuration = sparkConf.get("spark.art.window.duration").toLong
  val idleDurationThresholdPercentage = sparkConf.getDouble("spark.art.idle.threshold", DefaultIdleDurationThreshold)
  var idleDurationThreshold = math.round(idleDurationThresholdPercentage * windowDuration)

  val idealDrift = sparkConf.getLong("spark.art.ideal.drift", DefaultIdealDrift)
  val jitterTolerancePercentage = sparkConf.getDouble("spark.art.jitter.tolerance", DefaultJitterTolerance)
  var jitterTolerance = math.round(jitterTolerancePercentage * windowDuration)

  val predict = sparkConf.getBoolean("spark.art.predict", DefaultPredict)
  val accuracyStepPercentage = sparkConf.getDouble("spark.art.accuracy.step", DefaultAccuracyStep)
  var accuracyStep = math.round(accuracyStepPercentage * windowDuration).toInt

  val costStep = sparkConf.getInt("spark.art.cost.step", DefaultCostStep)
  val ingestionRateScaleFactor = sparkConf.getInt("spark.art.ir.scale.factor", DefaultIngestionRateScaleFactor)
  val spikeThresholdPercentage = sparkConf.getDouble("spark.art.spike.threshold", DefaultSpikeThreshold)
  var spikeThreshold = math.round(spikeThresholdPercentage * windowDuration)

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
  val accuracyTrainingInstances = new Instances("art-accuracy", attrs, 0)
  accuracyTrainingInstances.setClassIndex(attrs.indexOf("accuracy"))
  val accuracyClassifier = new SimpleLinearRegression
  // val classifierLinearRegression = new LinearRegression


  val costTrainingInstances = new Instances("art-cost", attrs, 0)
  costTrainingInstances.setClassIndex(attrs.indexOf("cost"))
  val costClassifier = new SimpleLinearRegression

  val lock = new Lock()
  var countUpdates = 0

  val policy = Policies.values.find(_.toString == sla.policy.getOrElse(DefaultPolicy.toString)).get

  private var log : Logger = null

  //println("ART MEMORY STATUS SIZE EXEC: " + ssc.sparkContext.getExecutorMemoryStatus.mkString(","))
  var cost = ssc.sparkContext.getExecutorMemoryStatus.size - 1

  var accuracy = 100
  var followingAccuracy = -1
  var delay: Long = -1
  var execTime: Long = -1
  var ingestionRate: Long = -1
  var deltaKnobEffect = 1
  var lastRoundTick: Long = -1
  var isUnstable = false
  var isSuperStable = false

  println(s"ART MANAGER ACTIVATED! (app: $appName, mode: $mode, policy: $policy, windowDuration: $windowDuration, " +
    s"idleDurationThreshold: $idleDurationThreshold, spikeThreshold: $spikeThreshold, jitterTolerance: $jitterTolerance, " +
    s"cost: $cost, accuracyStep: $accuracyStep)")


  // initialize stats file
  val statsFilename = s"stats-${System.currentTimeMillis()}-$appName-$idleDurationThreshold-$accuracyStep" +
    s"-$jitterTolerance-$idealDrift-$windowDuration.csv"
  val statsFile = new PrintWriter(statsFilename)
  statsFile.println("timestamp,ingestionRate,accuracy,cost,window,delay,execTime")


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
    for(c <- 1 to sla.maxCost.getOrElse(0) by 1) {
      if(c > 1.0) {
        ssc.sparkContext.requestExecutors(1)
      }

      for(a <- sla.minAccuracy.getOrElse(MaxAccuracy) to 100 by 10) {
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
      if(bd > 1000) {
        ssc.stop(false, true)
        setBatchDuration(bd)
        ssc.start()
      }

      val trials = 4
      for (t <- 1 to trials) {
        lock.acquire()
        println(s"ART profile:$bd,$windowDuration,$delay,$execTime")
      }
    })
  }

  def profileWorkloadForWindowDuration: Unit = {

    println(s"ART profile:batchDuration,window,delay,time")

    Seq(10,5,2).foreach(wd => {
      if(wd != 10) {
        setWindowDuration(Seconds(wd))
      }

      val trials = 2
      for (t <- 1 to trials) {
        lock.acquire()
        println(s"ART profile:$wd,$windowDuration,$delay,$execTime")
      }
    })
  }

  def predictCost: Boolean = {
    if(!predict) return false

    val ingestionRateXbps = (ingestionRate / ingestionRateScaleFactor).toInt
    val targetExecTime = (windowDuration - idealDrift)

    // TODO: To be able to make predictions, either the same ingestion rate was already seen
    // or a lower and higher ingestion rates were already seen
    if(seenMetrics.contains(LearningMetrics(ingestionRateXbps, accuracy, 0, windowDuration,
      targetExecTime / ingestionRateScaleFactor))) {

      println("ART This metric has already been seen!!!")
      //val predictedAccuracy = model.predict(Vectors.dense(ingestionRate, cost, windowDuration, targetExecTime))
      //accuracy = predictedAccuracy.toInt

      val instance = new DenseInstance(AttributeNames.length)
      instance.setDataset(costTrainingInstances)
      instance.setValue(0, accuracy)
      instance.setValue(1, ingestionRate.toDouble)
      instance.setValue(2, 0.0)
      instance.setValue(3, windowDuration.toDouble)
      instance.setValue(4, targetExecTime.toDouble)
      val predictedCost = costClassifier.classifyInstance(instance).toInt
      // val predictedAccuracyLR = classifierLinearRegression.classifyInstance(instance).toInt
      //println(s"ART RF: $predictAccuracy, LR: $predictedAccuracyLR")

      if(predictedCost < MinCost) {
        cost = MinCost
      } else if(predictedCost > sla.maxCost.get) {
        cost = sla.maxCost.get
      } else {
        cost = predictedCost - (predictedCost % costStep)
      }
      println(s"ART Predicted cost: $cost")
      return true
    }
    return false
  }

  def increaseCost: Boolean = {
    if(cost < sla.maxCost.getOrElse(MinCost)) {
      if(!predictCost) {

        // if it is a "small" spike
        //if(delay - (windowDuration + jitterTolerance) <= spikeThreshold) {
        if(execTime - (windowDuration + jitterTolerance) <= spikeThreshold) {
          cost = math.min(cost + costStep, sla.maxCost.get)
          ssc.sparkContext.requestExecutors(1)

          deltaKnobEffect = math.ceil(ExecutorBootDuration.toDouble / windowDuration.toDouble).toInt /
            reactWindowMultiple + 1
        } else {
          // number of rounds to wait until changes on this knob take effect
          val roundsToWait = math.ceil(delay.toDouble / windowDuration.toDouble).toInt
          // calculate accumulated excess of delay by the time changes on this knob take effect
          val delayExcess = delay - execTime + (execTime - windowDuration) * roundsToWait
          // target exec time that we need to have to make the system stable
          val targetExecTime = windowDuration - delayExcess
          // this gives an hint based on a linear workload
          val newCost = math.ceil(cost * execTime.toDouble / targetExecTime.toDouble).toInt

          val followingCost = math.min(math.ceil(cost * execTime.toDouble / (windowDuration - idealDrift).toDouble).toInt,
            sla.maxCost.get)

          val extraRoundsToWait = {
            if (newCost < 0 || newCost > sla.maxCost.get) {
              val minExecTime = (cost * execTime.toDouble / sla.maxCost.get.toDouble).toLong

              // delay excess by the time this knob effect is visible
              val followingDelayExcess = delayExcess + minExecTime - windowDuration

              val remaining = (windowDuration - minExecTime)
              // It's not possible to stabilize the workload with just the accuracy
              if (remaining < 0) {
                0
              } else {
                math.ceil(followingDelayExcess.toDouble / remaining.toDouble).toInt
              }
            } else {
              1
            }
          }
          cost = if(newCost < 0 || newCost > sla.maxCost.get) sla.maxCost.get else newCost

          println(s"ART Increasing cost to $cost (ts: ${System.currentTimeMillis()}})")
          deltaKnobEffect = (roundsToWait + extraRoundsToWait) / reactWindowMultiple + 1
          println(s"ART roundsToWait $roundsToWait, extraRoundsToWait: $extraRoundsToWait, followingCost: $followingCost, deltaKnobEffect: $deltaKnobEffect")
        }
      }
      return true
    }
    return false
  }

  def decreaseCost: Boolean = {
    if (sla.maxCost.isDefined && cost > MinCost) {
      if(!predictCost) {
        cost = math.max(cost - costStep, MinCost)
        println(s"ART Decreasing cost to $cost")
        println("ART TIME: " + System.currentTimeMillis())
        ssc.sparkContext.killExecutors(null)
        deltaKnobEffect = math.ceil(ExecutorBootDuration.toDouble / windowDuration.toDouble).toInt /
          reactWindowMultiple + 1
      }
      return true
    }
    return false
  }

  def predictAccuracy: Boolean = {
    if(!predict) return false
    
    val ingestionRateXbps = (ingestionRate / ingestionRateScaleFactor).toInt
    val targetExecTime = windowDuration - idealDrift

    // TODO: To be able to make predictions, either the same ingestion rate was already seen
    // or lower and higher ingestion rates were already seen
    if(seenMetrics.contains(LearningMetrics(ingestionRateXbps, 0, cost, windowDuration, targetExecTime / 1000))) {
      println("ART This metric has already been seen!!!")

      deltaKnobEffect = math.ceil(delay.toDouble / windowDuration.toDouble).toInt /
        reactWindowMultiple + 1

      //val predictedAccuracy = model.predict(Vectors.dense(ingestionRate, cost, windowDuration, targetExecTime))
      //accuracy = predictedAccuracy.toInt

      val instance = new DenseInstance(AttributeNames.length)
      instance.setDataset(accuracyTrainingInstances)
      instance.setValue(0, 0.0)
      instance.setValue(1, ingestionRate.toDouble)
      instance.setValue(2, cost.toDouble)
      instance.setValue(3, windowDuration.toDouble)
      instance.setValue(4, targetExecTime.toDouble)
      val predictedAccuracy = accuracyClassifier.classifyInstance(instance).toInt
      // val predictedAccuracyLR = classifierLinearRegression.classifyInstance(instance).toInt
      //println(s"ART RF: $predictAccuracy, LR: $predictedAccuracyLR")

      if(predictedAccuracy < sla.minAccuracy.get) {
        accuracy = sla.minAccuracy.get
      } else if(predictedAccuracy > MaxAccuracy) {
        accuracy = MaxAccuracy
      } else {
        accuracy = predictedAccuracy - (predictedAccuracy % accuracyStep)
      }
      println(s"ART Predicted accuracy: $accuracy")
      return true
    }

    return false
  }

  def increaseAccuracy : Boolean = {
    if(accuracy < MaxAccuracy) {
      if(!predictAccuracy) {
        accuracy = math.min(accuracy + accuracyStep, MaxAccuracy)
        println(s"ART Increasing accuracy to $accuracy ")
        deltaKnobEffect = 1 / reactWindowMultiple + 1
      }
      return true
    }
    return false
  }

  def decreaseAccuracy: Boolean = {
    if (accuracy > sla.minAccuracy.getOrElse(MaxAccuracy)) {
      if(!predictAccuracy) {

        // if it is a "small" spike
       // if(delay - (windowDuration + jitterTolerance) <= spikeThreshold) {
        if(execTime - (windowDuration + jitterTolerance) <= spikeThreshold) {
          accuracy = math.max(accuracy - accuracyStep, sla.minAccuracy.get)
          deltaKnobEffect = 1 / reactWindowMultiple + 1
        } else {
          // number of rounds to wait until changes on this knob take effect
          val roundsToWait = math.ceil(delay.toDouble / windowDuration.toDouble).toInt
          // calculate accumulated excess of delay by the time changes on this knob take effect
          val delayExcess = delay - execTime + (execTime - windowDuration) * roundsToWait
          // target exec time that we need to have to make the system stable
          val targetExecTime = windowDuration - delayExcess
          // this gives an hint based on a linear workload
          val newAccuracy = (accuracy * targetExecTime.toDouble / execTime.toDouble).toInt

          followingAccuracy = math.max((accuracy * (windowDuration - idealDrift).toDouble / execTime.toDouble).toInt,
            sla.minAccuracy.get)

          val extraRoundsToWait = {
            // either targetExecTime < 0 or necessary accuracy is below min accuracy
            if (newAccuracy < sla.minAccuracy.get) {
              val minExecTime = (execTime * sla.minAccuracy.get.toDouble / accuracy.toDouble).toLong

              // delay excess by the time this knob effect is visible
              val followingDelayExcess = delayExcess + minExecTime - windowDuration

              val remaining = (windowDuration - minExecTime)
              // It's not possible to stabilize the workload with just the accuracy
              if (remaining < 0) {
                0
              } else {
                math.ceil(followingDelayExcess.toDouble / remaining.toDouble).toInt
              }
            } else {
              1
            }
          }
          accuracy = if(newAccuracy < sla.minAccuracy.get) sla.minAccuracy.get else newAccuracy

          println(s"ART Decreasing accuracy to $accuracy")
          deltaKnobEffect = (roundsToWait + extraRoundsToWait) / reactWindowMultiple + 1
          println(s"ART roundsToWait $roundsToWait, extraRoundsToWait: $extraRoundsToWait, followingAccuracy: $followingAccuracy, deltaKnobEffect: $deltaKnobEffect")
        }
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

      // the system is only unstable if there are 2 consecutive unstable observations
      val isReallyUnstable = {
        if (delay > windowDuration + jitterTolerance) {
          if(isUnstable) {
            isUnstable = false
            true
          } else {
            isUnstable = true
            false
          }
        } else {
          isUnstable = false
          false
        }
      }

      // val isReallySuperStable = (windowDuration - execTime > idleDurationThreshold)
      val isReallySuperStable = (windowDuration - delay > idleDurationThreshold)
//      val isReallySuperStable = {
//        if (windowDuration - execTime > idleDurationThreshold) {
//          if(isSuperStable) {
//            isSuperStable = false
//            true
//          } else {
//            isSuperStable = true
//            false
//          }
//        } else {
//          isSuperStable = false
//          false
//        }
//      }

      // if(isReallyUnstable) {
      if(execTime > windowDuration + jitterTolerance) {
        println("ART Workload is unstable!")

        policy match {
          case MaximizeAccuracy =>
            if(!increaseCost && !decreaseAccuracy) {
              println("ART Nothing to do.")
            }

          case MinimizeCost =>
            if(!decreaseAccuracy && !increaseCost) {
              println("ART Nothing to do.")
            }
          case Balanced =>
        }

      } else if (isReallySuperStable) {

        policy match {
          case MaximizeAccuracy =>
            if (!increaseAccuracy && !decreaseCost) {
              println("ART Nothing to do.")
            }
          case MinimizeCost =>
            if(!decreaseCost && !increaseAccuracy) {
              println("ART Nothing to do.")
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

      if(mode == Profile) {
        // profileWorkload
        // System.exit(0)
        profileWorkloadForWindowDuration
        System.exit(0)
      } else if(mode == Execute){
        executeWorkload
      }
    }
  }.start()

  // implements speculation
//  new Thread {
//    override def run(): Unit = {
//      while(lastRoundTick < 0) Thread.sleep(1000)
//      while (true) {
//        val diff = System.currentTimeMillis() - lastRoundTick
//        val excess = diff - (windowDuration + jitterTolerance)
//
//        // if there is instability
//        if (excess > 0) {
//          println("ART Instability detected!")
//
//          if(excess > spikeThreshold) {
//
//            accuracy = sla.minAccuracy.get
//
//          } else {
//
//            accuracy -= accuracyStep
//            println(s"ART Decreasing accuracy to $accuracy")
//
//          }
//          deltaKnobEffect = 2
//        }
//        Thread.sleep(1000)
//      }
//    }
//  }

  def markAsSeen {
    if(!predict) return

    val ingestionRateXbps = (ingestionRate / ingestionRateScaleFactor).toInt
    val execTimeSec = execTime / ingestionRateScaleFactor
    if(seenMetrics.contains(LearningMetrics(ingestionRateXbps, accuracy, cost, windowDuration, execTimeSec))) {
      return
    }

    // mark as seen if there is nothing else art can do or if we are already operating within ideal conditions
    if((accuracy == MaxAccuracy && cost == MinCost && execTime <= windowDuration) ||
      (execTime >= (windowDuration - idleDurationThreshold) && execTime <= windowDuration)) {
      println("ART Marking as seen")

      for (ir <- ingestionRateXbps - 1 to ingestionRateXbps + 1) {
        for (et <- execTimeSec - 1 to execTimeSec + 1) {
          seenMetrics += LearningMetrics(ir, accuracy, 0, windowDuration, et)
          seenMetrics += LearningMetrics(ir, 0, cost, windowDuration, et)
        }
      }

      val newInstance = new DenseInstance(AttributeNames.length)
      newInstance.setDataset(accuracyTrainingInstances)
      newInstance.setValue(0, accuracy.toDouble)
      newInstance.setValue(1, ingestionRate.toDouble)
      newInstance.setValue(2, cost.toDouble)
      newInstance.setValue(3, windowDuration.toDouble)
      newInstance.setValue(4, execTime.toDouble)

      accuracyTrainingInstances.add(newInstance)
      accuracyClassifier.buildClassifier(accuracyTrainingInstances)
      // classifierLinearRegression.buildClassifier(trainingInstances)
    }
  }

  def updateExecutionTime(delay: Long, execTime: Long) {
    println(s"ART UPDATE: delay: $delay, execTime: $execTime, ingestionRate: $ingestionRate, accuracy: " +
      s"$accuracy, cost: $cost, windowDuration: $windowDuration")
    statsFile.println(s"%d,$ingestionRate,$accuracy,$cost,$windowDuration,$delay,$execTime"
      .format(System.currentTimeMillis()))
    statsFile.flush()

    this.delay = delay
    this.execTime = execTime

    println("ART NUMBER OF EXECUTORS: " + ssc.sparkContext.getExecutorMemoryStatus.size + ", TIME: " + System.currentTimeMillis())

    countUpdates += 1
    if(countUpdates == totalExecutions) {
      ssc.stop()
      System.exit(0)
    }

    lastRoundTick = System.currentTimeMillis()

    if (countUpdates % (reactWindowMultiple * deltaKnobEffect) == 0) {
      lock.release()
      deltaKnobEffect = 1
      markAsSeen
    }

    println(s"ART followingAccuracy: $followingAccuracy")
    if(followingAccuracy > 0 && (execTime + (delay - windowDuration) <= windowDuration)) {
      println(s"Setting accuracy to following accuracy: $followingAccuracy")
      accuracy = followingAccuracy
      followingAccuracy = -1
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

case class LearningMetrics(ingestionRate: Int, accuracy: Int, cost: Int, windowDuration: Long, execTime: Long)

case class SLA(application: String, maxExecTime: Double, accuracy: Option[Int], minAccuracy: Option[Int],
               cost: Option[Int], maxCost: Option[Int], policy: Option[String])

object SLA {
  implicit def SLACodecJson: CodecJson[SLA] =
    casecodec7(SLA.apply, SLA.unapply)("application", "maxExecTime", "accuracy", "minAccuracy",
      "cost", "maxCost", "policy")
}

trait RemoteArtManager extends Remote {
  @throws(classOf[RemoteException])
  def getAccuracy(): Double
}
