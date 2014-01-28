package scespet.util

import gsa.esg.mekon.core.EventSource
import scespet.core.{types, EventSourceX}
import scala.concurrent.duration.Duration

/**
 * todo: this needs to change such that there is only one event fired for a given pure time (i.e. two timers that coincide should be atomic)
 */
class Timer(duration:Duration) extends EventSourceX[Nothing] {
  var nextTime : Long = _

  override def init(startTime: Long, endTime: Long): Unit = {
    super.init(startTime, endTime)
    nextTime = startTime
  }

  def getNextTime: Long = nextTime

  def value: Nothing = null.asInstanceOf[Nothing]

  /**
   * @return the object to listen to in order to receive notifications of <code>value</code> changing
   */
  def trigger: types.EventGraphObject = this

  def hasNext(): Boolean = true

  def advanceState(): Unit = nextTime += duration.toMillis
}
