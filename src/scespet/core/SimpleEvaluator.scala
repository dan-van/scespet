package scespet.core

import collection.mutable.ArrayBuffer
import stub.gsa.esg.mekon.core.{EventGraphObject, Function => MFunc, Environment}
import stub.gsa.esg.mekon.core
import collection.mutable


/**
 * @version $Id$
 */

class SimpleEvaluator() extends FuncCollector {
  var eventI = 0
  var eventSourceIdx = 0

  val graph = new SlowGraphWalk

  var eventSources = mutable.Buffer[EventSource[_]]()

  def addEventSource[X](events: EventSource[X]) :EventSource[X] = {
    if (events.hasNext()) {
      eventSources += events
    } else {
      println("Empty event source added")
    }
    events
  }

  def run() {run(1000)}

  def run(n:Int) {
    val stopAt = eventI + n
    while (! eventSources.isEmpty && eventI < stopAt) {
      eventI += 1
      val nextSource = eventSources(eventSourceIdx)
      println(s"\nFiring event $eventI from $nextSource");
      nextSource.advance()
      graph.fire(nextSource)
      if (!nextSource.hasNext()) {
        eventSources.remove(eventSourceIdx)
      } else {
        eventSourceIdx += 1
      }
      if (eventSourceIdx >= eventSources.length) eventSourceIdx = 0
    }
  }

  def bind(src: EventGraphObject, sink: MFunc) {
//    println("bound "+src+" -> "+sink)
    graph.addTrigger(src, sink)
  }

  val env = new Environment {
    def wakeupThisCycle(target: core.Function) {
      graph.wakeup(target)
    }

    def addListener[T](source: Any, sink: EventGraphObject) {
      graph.addTrigger(source.asInstanceOf[EventGraphObject], sink.asInstanceOf[MFunc])
    }

    def removeListener[T](source: Any, sink: EventGraphObject) {
      graph.removeTrigger(source.asInstanceOf[EventGraphObject], sink.asInstanceOf[MFunc])
    }

    def hasChanged(trigger: Any):Boolean = {
      graph.hasChanged(trigger.asInstanceOf[EventGraphObject])
    }
  }


}

trait EventSource[X] extends HasVal[X] with EventGraphObject {
  def hasNext():Boolean
  def advance();
}

// I think this is a bit duff, basically it is a root "pipe", I don't think it really needs to be a function either
class IteratorEvents[X](val iterable:TraversableOnce[X]) extends EventSource[X] {
  val iterator = iterable.toIterable.iterator
  var value:X = _
  def hasNext() = iterator.hasNext
  def trigger = this
  def advance() {
    value = iterator.next()
  }
}
