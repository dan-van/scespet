package scespet.core

import collection.mutable
import gsa.esg.mekon.core.{EventGraphObject, EventSource}
import scespet.expression.{Scesspet, RootTerm, AbsTerm}

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 30/04/2013
 * Time: 22:54
 * To change this template use File | Settings | File Templates.
 */
class SimpleEnv() extends types.Env {
  var eventI = 0
  var eventSourceIdx = 0

  val graph = new SlowGraphWalk

  val eventSources = mutable.Set[EventSource]()
  val eventSourceQueue = mutable.Buffer[EventSource]()

  def addEventSource(events: gsa.esg.mekon.core.EventSource) {
    if (eventSources.add(events)) {
      if (events.hasNext()) {
        eventSourceQueue += events
      } else {
        println("Empty event source added")
      }
    }
  }


  def setStickyInGraph(source: EventGraphObject, sticky: Boolean) {
    if (sticky && source.isInstanceOf[EventSource]) {
      addEventSource(source.asInstanceOf[EventSource])
    }
  }

  def run() {run(1000)}

  def run(n:Int) {
    val stopAt = eventI + n
    while (! eventSourceQueue.isEmpty && eventI < stopAt) {
      eventI += 1
      val nextSource = eventSourceQueue(eventSourceIdx)
      println(s"\nFiring event $eventI from $nextSource, hasNext= ${nextSource.hasNext()}");
      nextSource.advanceState()
      graph.fire(nextSource)
      if (!nextSource.hasNext()) {
        eventSourceQueue.remove(eventSourceIdx)
      } else {
        eventSourceIdx += 1
      }
      if (eventSourceIdx >= eventSourceQueue.length) eventSourceIdx = 0
    }
  }

  def wakeupThisCycle(target: types.MFunc) {
    graph.wakeup(target)
  }

  def addListener[T](source: Any, sink: types.EventGraphObject) {
    if (source.isInstanceOf[EventSource]) {
      addEventSource(source.asInstanceOf[EventSource])
    }

    graph.addTrigger(source.asInstanceOf[types.EventGraphObject], sink.asInstanceOf[types.MFunc])
  }

  def removeListener[T](source: Any, sink: types.EventGraphObject) {
    graph.removeTrigger(source.asInstanceOf[types.EventGraphObject], sink.asInstanceOf[types.MFunc])
  }

  def hasChanged(trigger: Any):Boolean = {
    graph.hasChanged(trigger.asInstanceOf[EventGraphObject])
  }
}

