package scespet

import scespet.core._
import stub.gsa.esg.mekon.core.{Function, EventGraphObject}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scespet.core.types

/**
 * @author: danvan
 * @version $Id$
 */
class TermBuilder extends FuncCollector with types.Env {
  val roots = ArrayBuffer[types.EventGraphObject]()
  val graph = new SlowGraphWalk

  def query[X](stream: TraversableOnce[X]): MacroTerm[X] = {
    val eventSource = new IteratorEvents[X](stream)
    roots.append( eventSource )
    val initialTerm = new MacroTerm[X](this)(eventSource)
    return initialTerm
  }

  def bind(src: _root_.scespet.core.types.EventGraphObject, sink: _root_.scespet.core.types.MFunc) {
    graph.addTrigger(src, sink)
  }

  def addListener[T](source: Any, sink: EventGraphObject) {
    graph.addTrigger(source.asInstanceOf[EventGraphObject], sink.asInstanceOf[types.MFunc])
  }

  def removeListener[T](source: Any, sink: EventGraphObject) {
    graph.removeTrigger(source.asInstanceOf[EventGraphObject], sink.asInstanceOf[types.MFunc])
  }

  def hasChanged(trigger: Any): Boolean = ???
  def wakeupThisCycle(target: Function) {???}

  def copyInto(other:FuncCollector) {
    for (node <- graph.getAllNodes.asScala; listener <- node.getListeners.asScala) {
      other.env.addListener(node.getGraphObject, listener)
    }
  }

  def env = this
}
