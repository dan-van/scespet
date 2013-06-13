package scespet.core

import collection.mutable.ArrayBuffer
import collection.mutable
import scespet.expression.{AbsTerm, Scesspet, RootTerm}
import scespet.{EnvTermBuilder, expression}
import gsa.esg.mekon.core.EventSource.EventManagerInteractor
import gsa.esg.mekon.core
import core.{EventGraphObject, EventSource}


/**
 * @version $Id$
 */

class SimpleEvaluator() extends EnvTermBuilder(new SimpleEnv()) {
  def run(iter:Integer = 1000) {
    env.asInstanceOf[SimpleEnv].run(iter)
  }
}

trait EventSourceX[X] extends gsa.esg.mekon.core.EventSource with HasVal[X] {
  def hasNext():Boolean
  def advanceState()

  def setEventManagerInteractor(eventManagerInteractor: EventManagerInteractor) {}

  def isComplete: Boolean = hasNext()

  def init(startTime: Long, endTime: Long) {}
}

object IteratorEvents {
  def apply[X](iterable:TraversableOnce[X]) = {
    val eventCount = new Function1[Any, Long] { var i = 0L;  def apply(x:Any) : Long = {i +=1; i} }
    new IteratorEvents[X](iterable, eventCount)
  }
}

class IteratorEvents[X](val iterable:TraversableOnce[X], val timeGet:(X)=>Long) extends EventSourceX[X] {
  var peek:X = _
  val iterator = iterable.toIterable.iterator
  if (iterator.hasNext) peek = iterator.next()

  def hasNext() = peek != null

  def getNextTime: Long = timeGet(peek)

  var value:X = _
  def trigger = this
  def advanceState() {
    value = peek
    if (iterator.hasNext) {
      peek = iterator.next()
    } else {
      peek = null.asInstanceOf[X]
    }
  }

}
