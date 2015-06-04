package pt.inescid.gsd.art

import org.apache.spark.streaming.StreamingContext

/**
 * Created by sesteves on 03-06-2015.
 */
class ArtManager(ssc: StreamingContext) extends Runnable {

  var windowSize = 10000
  var delay: Long = -1
  var execTime: Long = -1


  def run() {
    while(true) {

      // if workload is not stable
      if(execTime > windowSize) {
        // add resources
        // ssc.checkpoint()
        ssc.stop()




        ssc.start()

      }

      Thread.sleep(1000)
    }
  }


  def updateExecutionTime(delay: Long, execTime: Long) {
    this.delay = delay
    this.execTime = execTime
  }

}
