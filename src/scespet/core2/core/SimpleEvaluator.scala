package scespet.core2.core

import collection.mutable.ArrayBuffer
import gsa.esg.mekon.core.{EventGraphObject, Function => MFunc}

/**
 * @author: danvan
 * @version $Id$
 */

class SimpleEvaluator() extends FuncCollector {
  def run() {
    var i = 0;
    while (eventSource.hasNext()) {
      advance()
      i += 1
    }
  }


  var funcs:IndexedSeq[MFunc] = new ArrayBuffer()

  var eventSource:EventSource[_] = _

  def events[X](iterable:Iterable[X]):Expr[X] = {
    if (eventSource != null) throw new IllegalArgumentException("SimpleEval can only handle one root event source")
    var asFunc = new IteratorAsFunc(iterable)
    eventSource = asFunc
    new Expr(asFunc)(this)
  }


  def bind(src: HasVal[_], sink: Func[_, _]) {
    println("bound "+src+" -> "+sink)
    funcs = funcs :+ sink
  }

  def bindSel(src: HasVal[_], sink: Select2[_]) {
    println("bound "+src+" -> "+sink)
    funcs = funcs :+ sink
  }

  def advance() = {
    eventSource.calculate()
    funcs.foreach(_.calculate())
  }
}

trait EventSource[X] extends Func[Null, X] {
  def hasNext():Boolean
}

// I think this is a bit duff, basically it is a root "pipe"
class IteratorAsFunc[X](val iterable:Iterable[X]) extends EventSource[X] {
  val iterator = iterable.iterator
  val source = new HasVal[Null] {def value = null; def trigger = null}
  def source_= (x:HasVal[Null]):Unit = throw new UnsupportedOperationException("can't set the source of a stream")
  var value:X = _

  def hasNext() = iterator.hasNext

  def calculate = {if (hasNext) {value = iterator.next(); true} else false}
}
