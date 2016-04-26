package scespet.core

import scespet.core.SliceCellLifecycle.{MutableBucketLifecycle, CellSliceCellLifecycle}

import reflect.macros.Context
import gsa.esg.mekon.core.{Function => MFunc, EventGraphObject, Environment}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag
import scespet.util._
import scespet.util.SliceAlign._



package object types {
  type Env = gsa.esg.mekon.core.Environment
  type EventGraphObject = gsa.esg.mekon.core.EventGraphObject
  type MFunc = gsa.esg.mekon.core.Function
  type EventSource = gsa.esg.mekon.core.EventSource

  case class Events(n:Int) extends AnyVal
  implicit class IntToEvents(val i:Int) extends AnyVal {
    def events = new Events(i)
  }


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
 * todo: rename me to ListenableHasVal
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

trait ToHasVal[T,X] {
  def toHasVal(t:T) :HasVal[X]
}

object ToHasVal {
  implicit class MacroTermToHasVal[X](term:MacroTerm[X]) extends ToHasVal[MacroTerm[X], X] {
    override def toHasVal(h: MacroTerm[X]): HasVal[X] = h.input
  }
}

object HasVal {
  // TODO: unify this with IsVal
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


// sealed, enum, blah blah
class ReduceType(val name:String) {}
object ReduceType {
  val CUMULATIVE = new ReduceType("Scan")
  val LAST = new ReduceType("Fold")
}


/**
 * Agg is an aggregation function.
 * it is mutable state (Cell), that is updated with calls to add (from CellAdder)
 *
 * todo - I think I should unify this with Bucket i.e. a base class will have complete() and value:Out
 * todo: I think I could split this into a 'Provides' interface
 */
trait Agg[-X] extends Cell with CellAdder[X] {
//  def add(x:X)
}

trait Cell {
  type OUT
  def value :OUT
}

/**
 * TODO: work out if this is necessary
 * This is the same as Agg, but with the output type parameter (V) explicit in the signature.
 * @tparam X
 * @tparam V
 */
trait Reducer[-X, V] extends Agg[X] with OutTrait[V] { // NODEPLOY - Agg[X] can be replaced by CellAdder[X]
  type OUT = V
}

/**
 * trait to add to a class to enable itself to the state that is exposed as a result of aggregating
 * @tparam X
 */
trait SelfAgg[-X] extends Agg[X] {
  type OUT = this.type
  override def value = this
  def add(x:X)
}

/**
 * The history of this is for integration with another codebase, where I have lots of MFunc-like classes
 * and I wanted to use them in a similar syntax to working with Agg aggregations.
 *
// todo - would be nicer to treat the 'Agg' trait and Bucket trait as compatible features that can be added to Cell
// todo - certainly the "bindTo" verb could work with a vanilla Cell
// NODEPLOY - could rename this to 'streamOf' ? and add a stop/start method relating to group/window operations?
  */
trait Bucket extends MFunc with AutoCloseable {
  def open():Unit
  /**
   * called after the last calculate() for this bucket. e.g. a median bucket could summarise and discard data at this point
   * NODEPLOY - rename to Close
   */
  def complete(){}

  /**
   * TODO: should I have distinct close, complete, reset? What happened to the idea of reset buckets?
   */
  override def close(): Unit = complete()

}


trait KeyToSliceCellLifecycle[K, C] {
  def lifeCycleForKey(k:K):SliceCellLifecycle[C]
}
object KeyToSliceCellLifecycle {
  private class KeyCellImplicit[K, Y](newCellF: K => Y, type_y:ClassTag[Y]) extends KeyToSliceCellLifecycle[K,Y]{
    override def lifeCycleForKey(k: K): SliceCellLifecycle[Y] = new CellSliceCellLifecycle[Y]( () => newCellF(k) )(type_y)

  }
  private class KeyBucketImplicit[K, B <: Bucket](newCellF: K => B, type_y:ClassTag[B]) extends KeyToSliceCellLifecycle[K,B]{
    override def lifeCycleForKey(k: K): SliceCellLifecycle[B] = new MutableBucketLifecycle[B]( () => newCellF(k) )(type_y)
  }

  // PONDER: I could work out how to use implicits here, but I only envision two usecases right now:
  def getKeyToSliceLife[K,C](newCellF:K=>C, type_c:ClassTag[C]): KeyToSliceCellLifecycle[K, C] = {
    if (classOf[Bucket].isAssignableFrom(type_c.runtimeClass)) {
      // I don't know what I'm doing....
      type CB = C with Bucket
      return new KeyBucketImplicit[K,CB](newCellF.asInstanceOf[K=>CB], type_c.asInstanceOf[ClassTag[CB]]).asInstanceOf[KeyToSliceCellLifecycle[K,C]]
    } else {
      return new KeyCellImplicit[K,C](newCellF, type_c)
    }
  }
}


trait Term[X] {
  implicit def eventObjectToHasVal[E <: types.EventGraphObject](evtObj:E) :HasVal[E] = new IsVal(evtObj)
//  implicit def toHasVal():HasVal[X]

  def value:X

//  def fold_all[Y <: Agg[X]](y: Y):Term[Y#OUT]
  def map[Y](f: (X) => Y, exposeNull:Boolean = true):Term[Y]
  def filter(accept: (X) => Boolean):Term[X]

  def reduce[Y, O](newBFunc: => Y)(implicit adder:Y => CellAdder[X], yOut :AggOut[Y, O], yType:ClassTag[Y]) :Term[O]

  def scan[Y, O](newBFunc: => Y)(implicit adder:Y => CellAdder[X], yOut :AggOut[Y, O], yType:ClassTag[Y]) :Term[O]

  def group[S](sliceSpec:S, triggerAlign:SliceAlign = AFTER)(implicit ev:SliceTriggerSpec[S]) :GroupedTerm[X]
  
  def window(window:HasValue[Boolean]) : GroupedTerm[X]

  def by[K](f: X => K) :MultiTerm[K,X]

  def valueSet[Y](expand: (X=>TraversableOnce[Y])) : VectTerm[Y,Y]

  def valueSet() : VectTerm[X,X] = valueSet(valueToSingleton[X])

  /**
   * emit an updated tuples of (this.value, y) when either series fires
   * yes, this is like 'zip', but because 'take' is similar I didn't want to use that term
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

  def filterType[T:ClassTag]:Term[T] = {
    filter( v => reflect.classTag[T].unapply(v).isDefined ).map(v => v.asInstanceOf[T])
  }

//  def filterType[T:Integer]():Term[T] = {
//    filter( v => reflect.classTag[T].unapply(v).isDefined ).map(v => v.asInstanceOf[T])
//  }

  //  private def valueToSingleton[X,Y] = (x:X) => Traversable(x.asInstanceOf[Y])
  private def valueToSingleton[Y] = (x:X) => Traversable(x.asInstanceOf[Y])
}

object MultiTerm {
  implicit def identTraversable[X] : X => TraversableOnce[X] = (x:X) => Traversable(x)
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

  def group[S](sliceSpec:S, triggerAlign:SliceAlign = AFTER)(implicit ev:VectSliceTriggerSpec[S]) :GroupedVectTerm[K, X]

//  def reduce[Y <: Agg[X]](newBFunc: K => Y):BucketBuilderVect[K, Y#OUT]
//  def reduce[Y <: Agg[X]](newBFunc: => Y):BucketBuilderVect[K, Y#OUT] = reduce[Y]((k:K) => newBFunc)

//  def reduce_all[Y <: Agg[X]](newBFunc: K => Y):VectTerm[K,Y#OUT]
//  def reduce_all[Y <: Agg[X]](newBFunc:  => Y):VectTerm[K,Y#OUT]  = reduce_all[Y]((k:K) => newBFunc)
//
  // NODEPLOY rename to scan
//  def fold[Y <: Agg[X]](newBFunc: K => Y):BucketBuilderVect[K, Y#OUT]
//  def fold[Y <: Agg[X]](newBFunc: => Y):BucketBuilderVect[K, Y#OUT] = fold[Y]((k:K) => newBFunc)
//  def fold_all[Y <: Agg[X]](reduceBuilder : K => Y):VectTerm[K,Y#OUT]
//  def fold_all[Y <: Agg[X]](reduceBuilder : => Y):VectTerm[K,Y#OUT]   = fold_all[Y]((k:K) => reduceBuilder)
}


