package scespet.core

import reflect.macros.Context
import stub.gsa.esg.mekon.core.{Function => MFunc, EventGraphObject, Environment}

package object types {
  type Env = stub.gsa.esg.mekon.core.Environment
  type EventGraphObject = stub.gsa.esg.mekon.core.EventGraphObject
  type MFunc = stub.gsa.esg.mekon.core.Function
}

trait FuncCollector {
  def bind(src:types.EventGraphObject, sink:types.MFunc)
  def env:types.Env
}

/**
 * Something that provides a value (i.e. a source)
 * @tparam X
 */
trait HasVal[X] {
  def value:X

  /**
   * @return the object to listen to in order to receive notifications of <code>value</code> changing
   */
  def trigger :types.EventGraphObject
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
trait Func[X,Y] extends HasVal[Y] with MFunc {
  // TODO: why is source necessary?
  var source:HasVal[X]
  def value:Y

  /**
   * This needs to be defined to allow someone to listen to me
   * @return
   */
  def trigger = this
  def calculate():Boolean
}

case class Events(n:Int)
case class Time(n:Int)

trait BucketBuilder[X,T] {
  def each(n:Int):MacroTerm[T]

  def window(windowStream: MacroTerm[Boolean]) :MacroTerm[T]

//  def all():MacroTerm[T]
//
//  def window(n:Events):MacroTerm[T]
//  def window(n:Time):MacroTerm[T]
//  def window(windowStream:MacroTerm[Boolean]):MacroTerm[T]
//
//  // todo: add a parameter for the input stream
//  def reset_pre(f:T=>Boolean):MacroTerm[T]
//  def reset_post(f:T=>Boolean):MacroTerm[T]
}

trait Reduce[X] {
  def add(x:X)
}

trait BucketTerm[X] {
  def newBucketBuilder[B](newB:()=>B):BucketBuilder[X, B]
}


