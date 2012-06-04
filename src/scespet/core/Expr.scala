package scespet.core

import gsa.esg.mekon.core.EventGraphObject

/**
 * @author: danvan
 * @version $Id$
 */


trait FuncCollector{
  def bind(src:HasVal[_], sink:Func[_,_])
}


/**
 * Something that provides a value (i.e. a source)
 * @tparam X
 */
trait HasVal[X] {
  def value:X
  def trigger :EventGraphObject
}

/**
 * something that has a source, and is a source (i.e. a pipe)
 * on "calculate", it may take from its source, and update its yielded value
 * if calculate returns false, then state was not modified
 *
 * i.e. this is an operation that can both map and filter
 * @tparam X
 * @tparam Y
 */
trait Func[X,Y] extends HasVal[Y] with gsa.esg.mekon.core.Function {
  var source:HasVal[X]
  def value:Y
  def trigger = this
  def calculate():Boolean
}


/**
 * this represents a binding of source -> sink
 * it provides operations to build subsequent bindings (e.g. "map")
 * it notifies a FuncCollector about all constructed bindings
 *
 * @param source
 * @param collector
 * @tparam X
 */
class Expr[X](val source:HasVal[X])(implicit collector:FuncCollector) {
  def filter(f:X => Boolean):Expr[X] = map(new AnonFilterFunc[X](f))

  def map[Y](f:X => Y):Expr[Y] = map(new AnonFunc(f))

  def map[Y](f:Func[X,Y]):Expr[Y] = {
    f.source = source
    collector.bind(source, f)
    return new Expr[Y](f)
  }

  def foreach(f:X => Unit) = {
    map(new AnonFunc(f))
  }
}



// integrated map and filter. map -> null implies filter
class AnonFunc[X,Y](val f:X => Y) extends AbsFunc[X,Y]{
  def calculate() :Boolean = {
    //    print("Computing "+f+" of "+source.value)
    value=f(source.value)
    return value != null
  }
}

class AnonFilterFunc[X](val f:X => Boolean) extends AbsFunc[X,X]{
  def calculate() = {
    if (f(source.value)) {
      value=source.value
      true
    } else {
      false
    }
  }
}

class IsVal[X <: EventGraphObject](val value:X) extends HasVal[X] {
  def trigger = value
}
