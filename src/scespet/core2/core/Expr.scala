package scespet.core2.core

import scala.reflect.runtime.universe._
import reflect.ClassTag
import stub.gsa.esg.mekon.core.core
import core.EventGraphObject

/**
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
trait Func[X,Y] extends HasVal[Y] with core.Function {
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
//  implicit def selectToFunc[Y <: Select[X]](select:Y) = {new SelectFunc[X,Y](select)}

  def map[Y](f:Func[X,Y]):Expr[Y] = {
    f.source = source
    collector.bind(source, f)
    return new Expr[Y](f)
  }

  def map[Y](f:X => Y):Expr[Y] = map(new AnonFunc(f))

//  def sel(select:Select[X]):Expr[select.Self] = map(new SelectFunc[X,select.Self](select))
  def sel[Y <: Select[X]](select:Y):Expr[Y] = {return new Expr[Y](new HasVal[Y] {
  def value: Y = select

  def trigger: EventGraphObject = null
})}

  def filter(f:X => Boolean):Expr[X] = map(new AnonFilterFunc[X](f))

  def sample(n:Long):Expr[X] = map(new SampledFunc[X](n))

  def foreach(f:X => Unit) = map(new AnonFunc(f))
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

/**
 * wrap an X as a Func[X,X]
 * @tparam X
 */
class Identity[X]() extends AbsFunc[X,X]{
  def calculate() = {
    value = source.value
    true
  }
}


/**
 * expose every nth sample of X as a Func[X,X]
 * @tparam X
 */
class SampledFunc[X](val n:Long) extends AbsFunc[X,X]{
  var counter = 0L
  def calculate() = {
    counter += 1
    if (counter % n == 0) {
      value = source.value
      true
    } else {
      false
    }
  }
}

/**
 *
 */
class SelectFunc[IN, OUT <: Select[_]](val select:OUT) extends Func[IN,OUT]{
  var source :HasVal[IN] = _
  def value = select

  def calculate() = {
    select.update()
    true
  }
}

class IsVal[X <: EventGraphObject](val value:X) extends HasVal[X] {
  def trigger = value
}


//abstract class Select[IN: TypeTag](implicit tag: TypeTag[IN]) extends DelayedInit {
abstract class Select[IN](implicit tag:ClassTag[IN]) extends DelayedInit {
  //    value = this
  var in:IN = _
//  type Me <: Select[IN]

  var initBody:()=>Unit = _

  override def delayedInit(x: => Unit) {
    initBody = ()=>{x;}
  }

  def update() {
    initBody()
  }

//  def apply():SelectFunc[IN,this.type] = {
//    return new SelectFunc[IN, Select.this.type](this)
//  }
}


