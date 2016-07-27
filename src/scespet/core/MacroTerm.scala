package scespet.core

import scespet.core.SliceCellLifecycle.{MutableBucketLifecycle, CellSliceCellLifecycle}
import scespet.core._
import scespet.util._
import scespet.util.SliceAlign._

import reflect.macros.Context
import scala.reflect.ClassTag
import gsa.esg.mekon.core.{Environment, EventGraphObject}
import gsa.esg.mekon.core.EventGraphObject.Lifecycle
import scala.concurrent.duration.Duration
import scespet.util.SliceAlign
import scespet.core.SliceTriggerSpec.MacroIsTriggerSpec
import scespet.core.types.{MFunc, Events}
import scespet.core.types.Events


/**
 * This wraps an input HasVal with API to provide typesafe reactive expression building
 */
class MacroTerm[X](val env:types.Env)(val input:HasVal[X]) extends Term[X] with HasVal[X] {
  import scala.language.experimental.macros
  import scala.collection.JavaConverters._

//  override implicit def toHasVal():HasVal[X] = input
  override def value = {
// NODEPLOY it would be handy to keep this, but init causes failure. can we work around this?
//  if (!env.hasChanged(trigger)) throw new AssertionError("interrogating value when !hasChanged is probably broken")
  input.value
}
  override def initialised: Boolean = input.initialised
  override def trigger: types.EventGraphObject = input.trigger

  /**
   * Fold is a running reduction - the new state of the reduction is exposed after each element addition
   * e.g. a running cumulative sum, as opposed to a sum
   * @param y
   * @tparam Y
   * @return
   */
  def fold_all[Y <: Agg[X]](y: Y):MacroTerm[Y#OUT] = {
    val listener = new AbsFunc[X,Y#OUT] {
      override def value = y.value
      initialised = true
      def calculate() = {
        y.add(input.value);
        true
      }
    }
    env.addListener(input.trigger, listener)
    return new MacroTerm[Y#OUT](env)(listener)
  }

  /**
   * NODEPLOY - is this replaced by 'reduce'?  NOTE, the implementation is far simpler here, may be worth keeping.
   *
   * Reduce collapses all values into a single value emitted at env.terminationEvent
   * @param y
   * @tparam Y
   * @return
   */
  def reduce_all[Y <: Agg[X]](y: Y):MacroTerm[Y#OUT] = {
    val listener = new AbsFunc[X,Y#OUT] {
      val reduction = y
      value = null.asInstanceOf[Y#OUT]

      val termination = env.getTerminationEvent()
      env.addListener(termination, this)

      def calculate() = {
        if (env.hasChanged(termination)) {
          value = reduction.asInstanceOf[Y].value
          initialised = true
          true
        } else {
          y.add(input.value)
          false
        }
      }
    }
    env.addListener(input.trigger, listener)
    return new MacroTerm[Y#OUT](env)(listener)
  }

  def map[Y](f: (X) => Y, exposeNull:Boolean = true):MacroTerm[Y] = {
    val listener = new AbsFunc[X,Y] {
      def calculate() = {
        val newVal = f(input.value)
        if (exposeNull || newVal != null.asInstanceOf[Y]) {
          value = newVal
          initialised = true
          true
        } else {
          false
        }
      }
    }
    env.addListener(input.trigger, listener)
    if (input.initialised) // This initialises the initial value - we do not need to propagate an event here.
      listener.calculate()
    return new MacroTerm[Y](env)(listener)
  }

  def filter(accept: (X) => Boolean):MacroTerm[X] = {
    class FilteredValue extends UpdatingHasVal[X] {
      var value = null.asInstanceOf[X]
      if (input.initialised) {
        env.fireAfterChangingListeners(this)
      }
      def calculate() = {
        if (accept(input.value)) {
          value = input.value
          initialised = true
          true
        } else {
          false
        }
      }
    }
    val listener: FilteredValue = new FilteredValue
    env.addListener(input.trigger, listener)
    return new MacroTerm[X](env)(listener)
  }

//  override def by[K](f: X => K) : VectTerm[K,X] = macro ByMacro.by[K,X]
  /**
   * present this single stream as a vector stream, where f maps value to key
   * effectively a demultiplex operation
   * @param f
   * @tparam K
   * @return
   */
  def by[K](f: X => K) : VectTerm[K,X] = {
    val vFunc: GroupFunc[K, X] = new GroupFunc[K, X](input, f, env)
//    env.addListener(input.trigger, vFunc)
    return new VectTerm[K, X](env)(vFunc)
  }

  def takef[Y](newGenerator:(X)=>HasVal[Y]) : VectTerm[X,Y] = {
    this.by(x => x).keyToStream(newGenerator)
  }

// Take a stream of X (usually representing some collection), map X -> Collection[Y] and generate a new flattened set of Y (mainly used if X is some form of collection and you want to flatten it)
//  def values[X1 <: ClassTag[X]]() : VectTerm[X1,X1] = values[X1](x => Traversable(x.asInstanceOf[X1]))

//  def distinct2[Y : ClassTag]() = values[Y](x => {Traversable[Y](x.asInstanceOf[Y])} )

  /**
   * generate a new vector, keyed by the values in this stream.
   * optionally provide a function that takes a value in the stream and yields an iterator to be used for generating many keys from one value
   * (this is especially useful if your values in the stream are Collections, as this allows you to flatten a Stream[Set[String]] -> Vect[String,String]
   *
   * todo: hmm, with identity as the expansion, this is the same as .by(_)
   *
   * @param expand
   * @tparam Y
   * @return
   */
  def valueSet[Y](expand: (X=>TraversableOnce[Y])) : VectTerm[Y,Y] = {
//    def valueSet[Y](expand: (X=>TraversableOnce[Y]) = valueToSingleton[X,Y] ) : VectTerm[Y,Y] = {
    // I doubt this is ever constructed with an initial value to be expanded
    val initial = if (input.value != null) {
      println("ODD, input seems to already be initialised")
      expand(input.value)
    } else {
      List[Y]()
    }

//    val typeY = reflect.classTag[Y]
//    val javaClass : Class[Y] = typeY.runtimeClass.asInstanceOf[Class[Y]]
    val flattenedSet = new MutableVector[Y](initial.toIterable.asJava, env) with types.MFunc {
      env.addListener(input.trigger, this)

      def calculate(): Boolean = {
        var changed = false
        if (input.value != null) {
          var toAdd = expand(input.value)
          changed = toAdd.map(add(_)).exists(_ == true)
        }
        changed
      }
    }
    return new VectTerm[Y, Y](env)(flattenedSet)
  }

  /**
   * emit an updated tuples of (this.value, y) when either series fires
   */
  def join[Y](y:MacroTerm[Y]):MacroTerm[(X,Y)] = {
    val listener = new UpdatingHasVal[(X,Y)] {
      var value:(X,Y) = _

      def calculate() = {
        var aY:Y = y.input.value
        value = (input.value, aY)
        initialised = true
        true
      }
    }
    env.addListener(input.trigger, listener)
    env.addListener(y.input.trigger, listener)
    return new MacroTerm[(X,Y)](env)(listener)
  }

  /**
   * Sample this series each time {@see y} fires, and emit tuples of (this.value, y)
   */
  def take[Y](y:MacroTerm[Y]):MacroTerm[(X,Y)] = {
    val listener = new UpdatingHasVal[(X,Y)] {
      var value:(X,Y) = _

      def calculate() = {
        value = (input.value, y.input.value)
        initialised = true
        true
      }
    }
    env.addListener(input.trigger, listener)
    return new MacroTerm[(X,Y)](env)(listener)
  }

  /**
   * Sample this series each time {@see y} fires, and emit tuples of (this.value, y)
   */
  def sample(evt:EventGraphObject):MacroTerm[X] = {
    val listener = new Generator[X](null.asInstanceOf[X], input.value)
    env.addListener(evt, listener)
    return new MacroTerm[X](env)(listener)
  }

  def reduce[Y, O](newBFunc: => Y)(implicit adder:Y => CellAdder[X], yOut :AggOut[Y, O], yType:ClassTag[Y]) :Term[O] = {
    // THINK: this could be special cased to provide a more performant impl of scanning a stream without grouping
    group[Null](null, AFTER)(SliceTriggerSpec.TERMINATION).reduce(newBFunc)(adder, yOut, yType).asInstanceOf[Term[O]]
  }

  def scan[Y, O](newBFunc: => Y)(implicit adder:Y => CellAdder[X], yOut :AggOut[Y, O], yType:ClassTag[Y]) :Term[O] = {
    // THINK: this could be special cased to provide a more performant impl of scanning a stream without grouping
    group[Null](null, AFTER)(SliceTriggerSpec.NULL).scan(newBFunc)(adder, yOut, yType).asInstanceOf[Term[O]]
  }

  def window(window:HasValue[Boolean]) : GroupedTerm[X] = {
    val uncollapsed = new UncollapsedGroup[X] {

      override def newBucket[B, OUT](reduceType: ReduceType, lifecycle: SliceCellLifecycle[B], cellOut: AggOut[B, OUT], bindings: List[(HasVal[_], (B) => Function[_, Unit])], exposeEmpty:Boolean): SlicedBucket[B, OUT] = {
// WindowedReduce is the non-event version, the other ones are now feature equivalent, not sure if I should just delete the simpler WindowedReduce version yet
//    if (! classOf[MFunc].isAssignableFrom( lifecycle.C_type.runtimeClass) ) {
//      //  val cell = new WindowedReduce[X, A, OUT](input, adder, cellOut, window, lifecycle, reduceType, env)
//    } else {

        reduceType match {
          case ReduceType.CUMULATIVE => new WindowedBucket_Continuous[B, OUT](cellOut, window, lifecycle, bindings, env, exposeEmpty)
          case ReduceType.LAST => new WindowedBucket_LastValue[B, OUT](cellOut, window, lifecycle, bindings, env, exposeEmpty)
        }
      }

    }
    new GroupedTerm(this, uncollapsed, env)
  }

  // NODEPLOY rename SliceAlign to TriggerAlign, with values = OPEN/CLOSE
  def group[S](sliceSpec:S, triggerAlign:SliceAlign = AFTER)(implicit ev:SliceTriggerSpec[S]) :GroupedTerm[X] = {
    val uncollapsed = new UncollapsedGroupWithTrigger[S, X](input, sliceSpec, triggerAlign, env, ev)
    new GroupedTerm[X](this, uncollapsed, env)
  }

  /**
   * bindTo is a mechanism to allow easier interop with lower-level mutable streams and existing business logic.
   * e.g. stats collectors that work on having mutator methods called from price and trade events, and having 'open' and 'close'
   * to mark bucket boundaries
   * NOTE: bucket is instantiated per key, but thereafer is mutated, unlike calls to scan and reduce
   */
  def bindTo[B <: Bucket, OUT](newBFunc: => B)(adder: B => X => Unit)(implicit aggOut :AggOut[B, OUT], type_b:ClassTag[B]) :PartialBuiltSlicedBucket[B,OUT] = {
    val groupedTerm = group[Any](null, AFTER)(SliceTriggerSpec.TERMINATION)

    val cellAdd:B => CellAdder[X] = (b:B) => new CellAdder[X] {
      override def add(x: X): Unit = adder(b)(x)
    }
    val lifecycle :SliceCellLifecycle[B] = new CellSliceCellLifecycle[B](() => newBFunc)(type_b)
    groupedTerm._collapse[B, OUT](lifecycle, cellAdd, aggOut)
  }
}

/**
 * NODEPLOY reame to BucketFactory
 * {@see scespet.core.UncollapsedVectGroup}
 * @tparam IN
 */
trait UncollapsedGroup[IN] {
  def newBucket[B, OUT](reduceType:ReduceType, lifecycle :SliceCellLifecycle[B], cellOut:AggOut[B, OUT], bindings:List[(HasVal[_], (B => _ => Unit))], exposeEmpty:Boolean) :SlicedBucket[B,OUT]
}

class UncollapsedGroupWithTrigger[S, IN](input:HasValue[_], sliceSpec:S, triggerAlign:SliceAlign, env:types.Env, ev: SliceTriggerSpec[S]) extends UncollapsedGroup[IN] {

  override def newBucket[B, OUT](reduceType:ReduceType, lifecycle :SliceCellLifecycle[B], cellOut:AggOut[B, OUT], bindings:List[(HasVal[_], (B => _ => Unit))], exposeEmpty:Boolean) :SlicedBucket[B,OUT] = {
    val sourceCell = input
    val sliceSpecEv = ev
// SlicedReduce is the non-event version, the other ones are now feature equivalent, not sure if I should just delete the simpler SlicedReduce version yet
//    if (! classOf[MFunc].isAssignableFrom( lifecycle.C_type.runtimeClass) ) {
//      //  val cell = new SlicedReduce[S, IN, B, OUT](sourceCell, adder, cellOut, sliceSpec, triggerAlign == BEFORE, lifecycle, reduceType, env, sliceSpecEv, exposeInitialValue = true)
//    } else {

    // note: exposeInitialValue = false because for a single stream, I don't want any downstream map operations to be applied
    // until first event arrives.
    // I'm a little unsure about this, perhaps it is an aspect of the query expression itself whether downstream
    // map oprerations should be a applied to an empty input value?

    // NODEPLOY clean up the instanceof hacks
    val doMutable = lifecycle.isInstanceOf[MutableBucketLifecycle[_]]

    val sliceBucket = triggerAlign match {
      case SliceAlign.AFTER if doMutable => {
        new SliceAfterBucket[S, B, OUT](cellOut, sliceSpec, lifecycle, reduceType, bindings, env, sliceSpecEv, exposeEmpty)
      }
      case SliceAlign.AFTER if !doMutable => {
        new SliceAfterSimpleCell[S, B, OUT](cellOut, sliceSpec, lifecycle, reduceType, bindings, env, sliceSpecEv, exposeEmpty)
      }
      case SliceAlign.BEFORE if doMutable => {
        new SliceBeforeBucket[S, B, OUT](cellOut, sliceSpec, lifecycle, reduceType, bindings, env, sliceSpecEv, exposeEmpty)
      }
      case SliceAlign.BEFORE if !doMutable => {
        new SliceBeforeSimpleCell[S, B, OUT](cellOut, sliceSpec, lifecycle.asInstanceOf[CellSliceCellLifecycle[B]], reduceType, bindings, env, sliceSpecEv, exposeEmpty)
      }
    }

    sliceBucket
  }

}

/**
 * NODEPLOY make this API symmetric with {@link scespet.core.GroupedVectTerm}
 */
class GroupedTerm[X](val term:MacroTerm[X], val uncollapsedGroup: UncollapsedGroup[X], val env:types.Env) {
//  def reduce[Y <: Cell](newBFunc: => Y) :Term[Y#OUT] = ???
//  def reduce[Y <: Cell](newBFunc: => Y)(implicit ev:Y <:< Agg[X]) :Term[Y#OUT] = {
  def reduce[Y, O](newBFunc: => Y)(implicit adder:Y => CellAdder[X], yOut :AggOut[Y, O], yType:ClassTag[Y]) :Term[O] = {
    val lifecycle :SliceCellLifecycle[Y] = new CellSliceCellLifecycle[Y](() => newBFunc)(yType)
    val exposeEmpty :Boolean = false
    _collapse[Y,O](lifecycle, adder, yOut).last(exposeEmpty)
  }

  def scan[Y, O](newBFunc: => Y, exposeEmpty :Boolean = false)(implicit adder:Y => CellAdder[X], yOut :AggOut[Y, O], yType:ClassTag[Y]) :Term[O] = {
    val lifecycle :SliceCellLifecycle[Y] = new CellSliceCellLifecycle[Y](() => newBFunc)(yType)
    _collapse[Y,O](lifecycle, adder, yOut).all(exposeEmpty)
  }

  def collapseWith2[B, OUT](newBFunc: => B)(addFunc: B => X => Unit)(implicit aggOut: AggOut[B, OUT], type_b:ClassTag[B]) :PartialBuiltSlicedBucket[B, OUT] = {
    collapseWith[B,OUT](()=>newBFunc)(addFunc)(aggOut, type_b)
  }

  def collapseWith[B, OUT](newBFunc: () => B)(addFunc: B => X => Unit)(implicit aggOut: AggOut[B, OUT], type_b:ClassTag[B]) :PartialBuiltSlicedBucket[B, OUT] = {
    val cellAdd:B => CellAdder[X] = (b:B) => new CellAdder[X] {
      override def add(x: X): Unit = addFunc(b)(x)
    }
    val lifecycle :SliceCellLifecycle[B] = new CellSliceCellLifecycle[B](newBFunc)(type_b)
    _collapse[B,OUT](lifecycle, cellAdd, aggOut)
  }

  def _collapse[B, OUT](lifecycle:SliceCellLifecycle[B], adder:B => CellAdder[X], yOut :AggOut[B, OUT]) :PartialBuiltSlicedBucket[ B, OUT] = {
    val groupWithBindings = new PartialBuiltSlicedBucket[B, OUT](uncollapsedGroup, yOut, lifecycle, env)
    val addX :(B) => (X) => Unit = (b:B) => (x:X) => {
      val ca = adder.apply(b)
      ca.add(x)
    }
    groupWithBindings.bind[X](term.input)(addX)
  }
}

// NODEPLOY rename. can this be a Term mixin for the partitioning operations?
class PartialBuiltSlicedBucket[Y, OUT](uncollapsed:UncollapsedGroup[_], cellOut:AggOut[Y,OUT], val cellLifecycle: SliceCellLifecycle[Y], val env:Environment) {
  var bindings = List[(HasVal[_], (Y => _ => Unit))]()

  def last(exposeEmpty :Boolean = false): MacroTerm[OUT] = {
    sealCollapse(ReduceType.LAST, exposeEmpty)
  }

  // NODEPLOY - delegate remaining Term interface calls here using lazyVal approach
  def all(exposeEmpty :Boolean = false): MacroTerm[OUT] = sealCollapse(ReduceType.CUMULATIVE, exposeEmpty)


  def bind[S](stream: HasVal[S])(adder: Y => S => Unit): PartialBuiltSlicedBucket[Y, OUT] = {
    bindings :+=(stream, adder)
    this
  }

  private def sealCollapse(reduceType:ReduceType, exposeEmpty :Boolean) :MacroTerm[OUT] = {
    val cell :SlicedBucket[Y,OUT] = uncollapsed.newBucket(reduceType, cellLifecycle, cellOut, bindings, exposeEmpty)
    new MacroTerm[OUT](env)(cell)
  }


  // NODEPLOY - delete this method:
  def reset[S](sliceSpec: S, triggerAlign: SliceAlign = AFTER)(implicit ev: SliceTriggerSpec[S]):PartialBuiltSlicedBucket[Y, OUT] = {
    // NODEPLOY move calls to this to be of the stream.group(slice).collapse pattern
    ???
  }
}


object MacroTerm {
  implicit def termToHasVal[E](term:MacroTerm[E]) :HasVal[E] = term.input
//  implicit def intToEvents(i:Int) = { Events(i) }
}





