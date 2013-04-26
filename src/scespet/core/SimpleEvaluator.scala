package scespet.core

import collection.mutable.ArrayBuffer
import gsa.esg.mekon.core.{EventGraphObject, Function => MFunc, Environment}
import collection.mutable
import scespet.expression.CollectingTerm


/**
 * @version $Id$
 */

class SimpleEvaluator() extends FuncCollector {
  var eventI = 0
  var eventSourceIdx = 0

  val graph = new SlowGraphWalk

  val eventSources = mutable.Set[EventSource[_]]()
  val eventSourceQueue = mutable.Buffer[EventSource[_]]()

  def addEventSource[X](events: EventSource[X]) :EventSource[X] = {
    if (eventSources.add(events)) {
      if (events.hasNext()) {
        eventSourceQueue += events
      } else {
        println("Empty event source added")
      }
    }
    events
  }

  def addExpression[X](data: TraversableOnce[X], term: CollectingTerm[X]) = {
    var events = new IteratorEvents[X](data)
    addEventSource(events)
    var root = new MacroTerm[X](this)(events)
    CollectingTerm.applyTree(term, root)
    SimpleEvaluator.this
  }




  def addRoot[X](root: Root[X]): MacroTerm[X] = {
    root.init(env)
//    val trigger: _root_.scespet.core.types.EventGraphObject = root.trigger
//    addEventSource(trigger.asInstanceOf[EventSource[_]])
    return new MacroTerm[X](this)(root)
  }

  def run() {run(1000)}

  def run(n:Int) {
    val stopAt = eventI + n
    while (! eventSourceQueue.isEmpty && eventI < stopAt) {
      eventI += 1
      val nextSource = eventSourceQueue(eventSourceIdx)
      println(s"\nFiring event $eventI from $nextSource, hasNext= ${nextSource.hasNext()}");
      nextSource.advance()
      graph.fire(nextSource)
      if (!nextSource.hasNext()) {
        eventSourceQueue.remove(eventSourceIdx)
      } else {
        eventSourceIdx += 1
      }
      if (eventSourceIdx >= eventSourceQueue.length) eventSourceIdx = 0
    }
  }

  def bind(src: EventGraphObject, sink: MFunc) {
//    println("bound "+src+" -> "+sink)
    if (src.isInstanceOf[scespet.core.EventSource[_]]) {
      addEventSource(src.asInstanceOf[scespet.core.EventSource[_]])
    }
    graph.addTrigger(src, sink)
  }

  val env = new Environment {
    def wakeupThisCycle(target: MFunc) {
      graph.wakeup(target)
    }

    def addListener[T](source: Any, sink: EventGraphObject) {
      if (source.isInstanceOf[scespet.core.EventSource[_]]) {
        addEventSource(source.asInstanceOf[scespet.core.EventSource[_]])
      }

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
