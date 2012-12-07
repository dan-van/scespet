package scespet.core

import collection.mutable.ArrayBuffer
import stub.gsa.esg.mekon.core.{EventGraphObject, Function => MFunc, Environment}
import stub.gsa.esg.mekon.core


/**
 * @version $Id$
 */

class SimpleEvaluator() extends FuncCollector {
  val graph = new SlowGraphWalk
  var eventSource:EventSource[_] = _


  def run() {
    var i = 0;
    while (eventSource.hasNext()) {
      println(s"\nFiring event $i");
      eventSource.advance()
      graph.fire(eventSource)
      i += 1
    }
  }

  def events[X](iterable:Iterable[X]):Expr[X] = {
    if (eventSource != null) throw new IllegalArgumentException("SimpleEval can only handle one root event source")
    var events: IteratorEvents[X] = new IteratorEvents[X](iterable)
    eventSource = events
    return new Expr(events)(this)
  }

  def bind(src: EventGraphObject, sink: MFunc) {
    println("bound "+src+" -> "+sink)
    graph.addTrigger(src, sink)
  }

  val env = new Environment {
    def wakeupThisCycle(target: core.Function) {
      graph.wakeup(target)
    }

    def addListener[T](source: Any, sink: EventGraphObject) {
      graph.addTrigger(source.asInstanceOf[EventGraphObject], sink.asInstanceOf[MFunc])
    }
  }


}

trait EventSource[X] extends HasVal[X] with EventGraphObject {
  def hasNext():Boolean
  def advance();
}

// I think this is a bit duff, basically it is a root "pipe", I don't think it really needs to be a function either
class IteratorEvents[X](val iterable:Iterable[X]) extends EventSource[X] {
  val iterator = iterable.iterator
  var value:X = _
  def hasNext() = iterator.hasNext
  def trigger = this
  def advance() {
    value = iterator.next()
  }
}
