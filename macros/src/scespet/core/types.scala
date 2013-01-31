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
  var source:HasVal[X]
  def value:Y
  def trigger = this
  def calculate():Boolean
}

trait BucketBuilder[T] {
  def each(n:Int):MacroTerm[T]
}

trait Reduce[X] {
  def add(x:X)
}

trait BucketTerm {
  def newBucketBuilder[B](newB:()=>B):BucketBuilder[B]
}


