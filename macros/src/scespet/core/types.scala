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
trait HasVal[X] extends types.EventGraphObject {
  def value:X

  /**
   * @return the object to listen to in order to receive notifications of <code>value</code> changing
   */
  def trigger :types.EventGraphObject
}

trait UpdatingHasVal[Y] extends HasVal[Y] with MFunc {
  /**
   * @return the object to listen to in order to receive notifications of <code>value</code> changing
   */
  def trigger = this
}

/**
 * trivial UpdatingHasVal which needs to be bound to an event source.
 * on trigger, it will apply function f and store the result
 * @param initVal
 * @param f
 * @tparam Y
 */
class Generator[Y](initVal:Y, f:()=>Y) extends UpdatingHasVal[Y] {
  var value = initVal

  def calculate():Boolean = {
    value = f()
    return true
  }
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
trait Func[X,Y] extends UpdatingHasVal[Y] {
  // TODO: why is source necessary?
  var source:HasVal[X]
  def value:Y

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

trait BucketBuilderVect[X, K, T] {
  def each(n:Int):VectTerm[K,T]

  /**
   * window the whole vector by a single bucket stream (e.g. 9:00-17:00 EU)
   * @param windowStream
   * @return
   */
  def window(windowStream: MacroTerm[Boolean]) :VectTerm[K, T]

  /**
   * window each element in the vector with the given window function
   * @return
   */
  def window(windowFunc: K => HasVal[Boolean]) :VectTerm[K, T]
}

trait Reduce[X] {
  def add(x:X)
}

trait BucketTerm[X] {
  def newBucketBuilder[B](newB:()=>B):BucketBuilder[X, B]
}


