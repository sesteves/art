package pt.inescid.gsd.art

import org.apache.spark.streaming.StreamingContext
import org.slf4j.Logger

/**
 * Created by sesteves on 03-06-2015.
 */
class ArtManager(ssc: StreamingContext) extends Runnable {

  println("ART MANAGER ACTIVATED!")


  private var log : Logger = null

  var windowSize = 4000
  var delay: Long = -1
  var execTime: Long = -1

  println("ART windowSize: " + windowSize)

  def run() {
    while(true) {

      println(s"ART Delay: $delay, ExecTime: $execTime")

      // if workload is not stable
      if(execTime > windowSize) {

        println("ART ExecTime > WindowSize")

        // add resources
        // ssc.checkpoint()
        println("ART STOPPING StreamingContext")
        // ssc.stop()

        println("ART Requesting one more executor")
        ssc.sparkContext.requestExecutors(1)

        println("ART STARTING StreamingContext")
        // ssc.start()

      }

      Thread.sleep(windowSize)
    }
  }


  def updateExecutionTime(delay: Long, execTime: Long) {

    println(s"ART updateExecutionTime called!")

    this.delay = delay
    this.execTime = execTime
  }

}
