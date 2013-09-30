package scespet.core

import reflect.macros.Context
import gsa.esg.mekon.core.{Function => MFunc, EventGraphObject, Environment}
import scala.reflect.ClassTag

package object types {
  type Env = gsa.esg.mekon.core.Environment
  type EventGraphObject = gsa.esg.mekon.core.EventGraphObject
  type MFunc = gsa.esg.mekon.core.Function
  type EventSource = gsa.esg.mekon.core.EventSource
}

/**
 * used to wrap a builder on 'HasVal' to allow FuncCollector to install roots into different environments
 * @param builder
 * @tparam X
 */
class Root[X](builder :types.Env => HasVal[X]) extends UpdatingHasVal[X] {
  var delegate :HasVal[X] = _

  def init(newEnv :types.Env) = {
    delegate = builder(newEnv)
    // downstream listeners are assuming I will fire when I have a new value.
    // to make this happen, we need to bind up a listener to our true underlier
    newEnv.addListener(delegate.trigger, this)
  }
  def value: X = delegate.value

  def calculate(): Boolean = true
}

trait FuncCollector {
  def addRoot[X](root:Root[X]) :MacroTerm[X]
  def bind(src:types.EventGraphObject, sink:types.MFunc)
  def env:types.Env
}

/**
 * Something that provides a value (i.e. a source)
 * @tparam X
 */
trait HasVal[X] extends HasValue[X]{
  def value:X

  /**
   * @return the object to listen to in order to receive notifications of <code>value</code> changing
   */
  def trigger :types.EventGraphObject

  def getTrigger: EventGraphObject = trigger
}

object HasVal {
  implicit def funcToHasVal[F <: EventGraphObject](f:F) = new HasVal[F] {
    val value = f
    def trigger = value.asInstanceOf[EventGraphObject]
  }
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

  def all():Term[T]
//
//  def window(n:Events):MacroTerm[T]
//  def window(n:Time):MacroTerm[T]
//  def window(windowStream:MacroTerm[Boolean]):MacroTerm[T]
//
  def slice_pre(trigger:EventGraphObject):MacroTerm[T]
  def slice_post(trigger:EventGraphObject):MacroTerm[T]
}

trait BucketBuilderVect[K, X, T] {
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
  def window(windowFunc: K => HasValue[Boolean]) :VectTerm[K, T]

  def slice_pre(trigger: EventGraphObject):VectTerm[K,T]

  /**
   * collect data into buckets that get 'closed' *after* the given event fires.
   * This is important if the same event can both be added to a bucket, and be responsible for closing the bucket.
   * e.g. bucket trades between trade events where the size is < median trade.
   *
   * @see reset_pre
   * @param trigger
   * @return
   */
  def slice_post(trigger: EventGraphObject):VectTerm[K,T]
}

trait Reduce[-X] extends Serializable {
  def add(x:X)
}

trait Term[X] {
  def value:X

  def fold_all[Y <: Reduce[X]](y: Y):Term[Y]
  def map[Y](f: (X) => Y):Term[Y]
  def filter(accept: (X) => Boolean):Term[X]

  def reduce[Y <: Reduce[X]](newBFunc: => Y):BucketBuilder[X, Y]

  def filterType[T:ClassTag]():Term[T] = {
    filter( v => reflect.classTag[T].unapply(v).isDefined ).map(v => v.asInstanceOf[T])
  }
}


trait BucketTerm[X] extends Term[X] {
  def newBucketBuilder[B](newB:()=>B):BucketBuilder[X, B]
}

trait BucketVectTerm[K,X] {
  def newBucketBuilder[B](newB:()=>B):BucketBuilderVect[K, X, B]
}


