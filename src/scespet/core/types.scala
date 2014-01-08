package scespet.core

import reflect.macros.Context
import gsa.esg.mekon.core.{Function => MFunc, EventGraphObject, Environment}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
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
    initialised = delegate.initialised
    // downstream listeners are assuming I will fire when I have a new value.
    // to make this happen, we need to bind up a listener to our true underlier
    newEnv.addListener(delegate.trigger, this)
  }
  def value: X = delegate.value

  def calculate(): Boolean = {
    initialised = true
    true
  }
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
  def initialised:Boolean

  def value:X

  /**
   * @return the object to listen to in order to receive notifications of <code>value</code> changing
   */
  def trigger :types.EventGraphObject

  // this is only here for Java compatibility
  def getTrigger: EventGraphObject = trigger
}

object HasVal {
  implicit def funcToHasVal[F <: EventGraphObject](f:F) = new HasVal[F] {
    val value = f
    def trigger = value.asInstanceOf[EventGraphObject]
    def initialised = true
  }
}

class IsVal[F <: EventGraphObject](f:F) extends HasVal[F] {
  val value = f
  def trigger = f
  def initialised = true
}

trait UpdatingHasVal[Y] extends HasVal[Y] with MFunc {
  /**
   * @return the object to listen to in order to receive notifications of <code>value</code> changing
   */
  def trigger = this
  var initialised = false
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
  initialised = true

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

// sealed, enum, blah blah
class ReduceType(val name:String) {}
object ReduceType {
  val CUMULATIVE = new ReduceType("Fold")
  val LAST = new ReduceType("Fold")
}

trait BucketBuilder[X,T] {
  def each(n:Int):Term[T]

  /**
   * define buckets by transitions from true->false in a boolean stream.
   * i.e. while 'windowStream' value is true, add to bucket.
   * Close the bucket on true -> false transition.
   * Open a new bucket on false ->  true transition
   *
   * this is useful for effectively constructing 'while' aggregations.
   * e.g. tradeSize.reduce(new Sum).window( continuousTrading )
   * @param windowStream
   * @return
   */
  def window(windowStream: Term[Boolean]) :Term[T]

  def all():Term[T]
//
//  def window(n:Events):MacroTerm[T]
//  def window(n:Time):MacroTerm[T]
//  def window(windowStream:MacroTerm[Boolean]):MacroTerm[T]
//
  // todo: think about how to build an implicit conversion from eventGraphObject -> Term
  // todo: if we have a Builder instance in scope, then it is possible with implicits
  def slice_pre(trigger:EventGraphObject):MacroTerm[T]
  def slice_post(trigger:EventGraphObject):MacroTerm[T]
  def slice_pre(trigger:MacroTerm[_]):MacroTerm[T]
  def slice_post(trigger:MacroTerm[_]):MacroTerm[T]
}

trait BucketBuilderVect[K, T] {
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

  /**
   * do a takef on the given vector to get hasValue[Boolean] for each key in this vector.
   * if the other vector does not have the given key, the window will be assumed to be false (i.e. not open)
   * @return
   */
  def window(windowVect: VectTerm[K,Boolean]) :VectTerm[K, T]

  /**
   * collect data into buckets that get 'closed' *before* the given event fires.
   * This is important if the same event can both be added to a bucket, and be responsible for closing the bucket.
   * e.g. bucket trades into buckets created whenever the trade direction changes
   *
   * @see #slice_post
   * @param trigger
   * @return
   */
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

// TODO: I reckon this should also expose a 'value' type
trait Reduce[-X] extends Serializable {
  def add(x:X)
}

// todo - I think I want to merge Reduce and Bucket
//trait Bucket extends {
trait Bucket extends types.MFunc with Serializable {
  /**
   * called after the last calculate() for this bucket. e.g. a median bucket could summarise and discard data at this point
   */
  def complete(){}
}

trait Term[X] {
  implicit def eventObjectToHasVal[E <: types.EventGraphObject](evtObj:E) :HasVal[E] = new IsVal(evtObj)

  def value:X

  def fold_all[Y <: Reduce[X]](y: Y):Term[Y]
  def map[Y](f: (X) => Y, exposeNull:Boolean = true):Term[Y]
  def filter(accept: (X) => Boolean):Term[X]

  def reduce_all[Y <: Reduce[X]](y: Y):Term[Y]
  def reduce[Y <: Reduce[X]](newBFunc: => Y):BucketBuilder[X, Y]

  def fold[Y <: Reduce[X]](newBFunc: => Y):BucketBuilder[X, Y]

  def by[K](f: X => K) :MultiTerm[K,X]

  def valueSet[Y](expand: (X=>TraversableOnce[Y])) : VectTerm[Y,Y]

  def valueSet() : VectTerm[X,X] = valueSet(valueToSingleton[X])

  /**
   * emit an updated tuples of (this.value, y) when either series fires
   */
  def join[Y](y:MacroTerm[Y]):MacroTerm[(X,Y)]

  /**
   * Sample this series each time {@see y} fires, and emit tuples of (this.value, y)
   */
  def take[Y](y:MacroTerm[Y]):MacroTerm[(X,Y)]

  /**
   * Sample this series each time {@see y} fires, and emit tuples of (this.value, y)
   */
  def sample(evt:EventGraphObject):MacroTerm[X]

  def filterType[T:ClassTag]():Term[T] = {
    filter( v => reflect.classTag[T].unapply(v).isDefined ).map(v => v.asInstanceOf[T])
  }

//  def filterType[T:Integer]():Term[T] = {
//    filter( v => reflect.classTag[T].unapply(v).isDefined ).map(v => v.asInstanceOf[T])
//  }

  //  private def valueToSingleton[X,Y] = (x:X) => Traversable(x.asInstanceOf[Y])
  private def valueToSingleton[Y] = (x:X) => Traversable(x.asInstanceOf[Y])
}

trait MultiTerm[K,X] {
  implicit def eventObjectToHasVal[E <: types.EventGraphObject](evtObj:E) :HasVal[E] = new IsVal(evtObj)

  /**
   * for symmetry with MacroTerm.value
   * @return
   */
  def value = values

  def values:List[X]
  def keys:List[K]

  def apply(k:K):MacroTerm[X]

  /**
   * todo: call this "filterKey" ?
   */
  def subset(predicate:K=>Boolean):VectTerm[K,X]

  /**
   * I think this concept is wrong. May need a special type for it (e.g. MatrixTerm[(K2,K), X])
   * I think a nested composite key is different to simply a Map of key tuples to values.
   */
  def by[K2]( keyMap:K=>K2 ):VectTerm[K2,X]

  /**
   * This allows operations that operate on the entire vector rather than single cells (e.g. a demean operation, or a "unique value count")
   * I want to think more about other facilities on this line
   *
   * @return
   */
  def mapVector[Y](f:VectorStream[K,X] => Y):MacroTerm[Y]

  def map[Y: TypeTag](f:X=>Y, exposeNull:Boolean = true):VectTerm[K,Y]

  def filterType[Y : ClassTag]():VectTerm[K,Y]

  def filter(accept: (X) => Boolean):VectTerm[K,X]

  /**
   * used to build a set from the values in a vector
   * the new vector acts like a set (key == value), generated values are folded into it.
   *
   * todo: maybe call this "flatten", "asSet" ?
   */
  def toValueSet[Y]( expand: (X=>TraversableOnce[Y]) = ( (x:X) => Traversable(x.asInstanceOf[Y]) ) ):VectTerm[Y,Y]

  /**
   * derive a new vector by applying a function to the keys of the current vector.
   * The new vector will have the same keys, but different values.
   *
   * I used to provide both key and value as inputs to the cell build, but removed this.
   * If it is necessary to use the current value of a cell to build the derived cells one can do this:
   * vect.derive(key => new MyCell(key, vect(key).value ))
   *
   * TODO: naming
   * this is related to "map", but "map" is a function of value, this is a function of key
   * maybe this should be called 'mapkey', or 'takef'? (i.e. we're 'taking' cells from the domain 'cellFromKey'?)
   * the reason I chose 'join' is that we're effectively doing a left join of this vector onto a vector[domain, cellFromKey]
   @param cellFromKey
    * @tparam Y
   * @return
   */
  def keyToStream[Y]( cellFromKey:K=>HasVal[Y] ):VectTerm[K,Y]
  def join[Y, K2]( other:VectTerm[K2,Y], keyMap:K => K2) :VectTerm[K,(X,Y)]
  def join[Y]( other:VectTerm[K,Y] ):VectTerm[K,(X,Y)] = join(other, identity)

  def sample(evt:EventGraphObject):VectTerm[K,X]

  def reduce[Y <: Reduce[X]](newBFunc: K => Y):BucketBuilderVect[K, Y]
  def reduce[Y <: Reduce[X]](newBFunc: => Y):BucketBuilderVect[K, Y] = reduce[Y]((k:K) => newBFunc)

  def reduce_all[Y <: Reduce[X]](newBFunc: K => Y):VectTerm[K,Y]
  def reduce_all[Y <: Reduce[X]](newBFunc:  => Y):VectTerm[K,Y]  = reduce_all[Y]((k:K) => newBFunc)

  def fold[Y <: Reduce[X]](newBFunc: K => Y):BucketBuilderVect[K, Y]
  def fold[Y <: Reduce[X]](newBFunc: => Y):BucketBuilderVect[K, Y] = fold[Y]((k:K) => newBFunc)
  def fold_all[Y <: Reduce[X]](reduceBuilder : K => Y):VectTerm[K,Y]
  def fold_all[Y <: Reduce[X]](reduceBuilder : => Y):VectTerm[K,Y]   = fold_all[Y]((k:K) => reduceBuilder)
}


