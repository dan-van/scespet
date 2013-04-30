package scespet.core

import collection.mutable.ArrayBuffer
import collection.mutable
import scespet.expression.{AbsTerm, Scesspet, RootTerm}
import scespet.expression
import gsa.esg.mekon.core.EventSource.EventManagerInteractor
import gsa.esg.mekon.core
import core.{EventGraphObject, EventSource}


/**
 * @version $Id$
 */

class SimpleEvaluator() extends types.Env {
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

  def addRoot[X](root: Root[X]): MacroTerm[X] = {
    root.init(this)
//    val trigger: _root_.scespet.core.types.EventGraphObject = root.trigger
//    addEventSource(trigger.asInstanceOf[EventSource[_]])
    return new MacroTerm[X](this)(root)
  }

  def query[X](data: EventSourceX[X]) : MacroTerm[X] = {
    return new MacroTerm[X](this)(data)
  }

  private def buildLocalCopy[X,Y](value: AbsTerm[X, Y]) :Term[Y] = {
    if (value.parent != null) {
      val localParent = buildLocalCopy(value.parent)
      val localThis = value.applyTo(localParent)
      localThis
    } else if (value.isInstanceOf[ RootTerm[Y] ]) {
      var hasVal = value.asInstanceOf[RootTerm[Y]].buildHasVal(this)
      new MacroTerm[Y](this)(hasVal)
    } else {
      ???
    }
  }

  def run(scesspet: Scesspet) {
    for (leaf <- scesspet.leaves) {
      val newInstance = buildLocalCopy(leaf)
    }
  }

  def run[X](term: Term[X]) :collection.mutable.Buffer[X] = {
    term match {
      case absTerm:AbsTerm[_,X] => {
        var localTerm = buildLocalCopy(absTerm)
        // add a reduce of the data -> Buffer t store and return final results
        val output = collection.mutable.Buffer[X]()
        localTerm.map[Unit](x => {output += x;})
        run()
        output
      }
      case _ => ???
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

  def bind(src: EventGraphObject, sink: types.MFunc) {
//    println("bound "+src+" -> "+sink)
    if (src.isInstanceOf[types.EventSource]) {
      addEventSource(src.asInstanceOf[types.EventSource])
    }
    graph.addTrigger(src, sink)
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

trait EventSourceX[X] extends HasVal[X] with gsa.esg.mekon.core.EventSource {
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
