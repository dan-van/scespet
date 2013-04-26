package scespet

import scespet.core._
import gsa.esg.mekon.core.{Function, EventGraphObject}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scespet.core.types

/**
 * @version $Id$
 */
class TermBuilder extends FuncCollector with types.Env {
  val roots = ArrayBuffer[Root[_]]()
  val graph = new SlowGraphWalk

  def query[X](stream: TraversableOnce[X]): MacroTerm[X] = {
    var root = new Root( _ => new IteratorEvents[X](stream) )
    return addRoot(root)
  }

  def addRoot[X](root: Root[X]): MacroTerm[X] = {
    roots.append(root)
    val initialTerm = new MacroTerm[X](this)(root)
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
    for (root <- roots) {
      other.addRoot(root)
    }
    for (node <- graph.getAllNodes.asScala; listener <- node.getListeners.asScala) {
      other.env.addListener(node.getGraphObject, listener)
    }
  }

  def env = this
}
