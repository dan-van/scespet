package scespet.core

import scespet.core._
import scespet.util._

import reflect.macros.Context
import scala.reflect.ClassTag
import gsa.esg.mekon.core.{Environment, EventGraphObject}
import gsa.esg.mekon.core.EventGraphObject.Lifecycle
import scala.concurrent.duration.Duration
import scespet.util.SliceAlign
import scespet.core.SliceTriggerSpec.MacroIsTriggerSpec
import scespet.core.types.Events


/**
 * This wraps an input HasVal with API to provide typesafe reactive expression building
 */
class MacroTerm[X](val env:types.Env)(val input:HasVal[X]) extends Term[X] {
  import scala.language.experimental.macros
  import scala.collection.JavaConverters._

  def value = input.value

  // this will be a synonym for fold(Y).all
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
   * Fold is a running reduction - the new state of the reduction is exposed after each element addition
   * e.g. a running cumulative sum, as opposed to a sum
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

  def reduce[Y <: Agg[X]](newBFunc: => Y):BucketBuilder[X, Y#OUT] = new BucketBuilderImpl[X,Y](() => newBFunc, MacroTerm.this, ReduceType.LAST, env)

  def fold[Y <: Agg[X]](newBFunc: => Y):BucketBuilder[X, Y#OUT] = new BucketBuilderImpl[X,Y](() => newBFunc, MacroTerm.this, ReduceType.CUMULATIVE, env)

  sealed class FireAct(name:String) {
  }
  object FireAct {
    val Close = new FireAct("Close")
    val Open = new FireAct("Open")
  }

  // NODEPLOY rename to reduce
  def reduce2[Y <: Agg[X]](newBFunc: => Y) = new PartialAggOrAcc[X, Y](input, () => newBFunc, env)
}

class GroupedTerm[X, R <: Agg[X]](reduceBuilder:(ReduceType)=>SlicedReduce[X, R], val env:types.Env)(val input:HasVal[X]) {
  private lazy val scanTerm :MacroTerm[R#OUT] = {
    val slicer = reduceBuilder(ReduceType.CUMULATIVE)
    new MacroTerm[R#OUT](env)(slicer)
  }

  def last() = {
    val slicer = reduceBuilder(ReduceType.LAST)
    new MacroTerm[R#OUT](env)(slicer)
  }

  // NODEPLOY - evaporate this method by using delegation onto lazyVal
  def all() = scanTerm
}

// NODEPLOY this should be a Term that also supports partitioning operations
class PartialAggOrAcc[X, Y <: Agg[X]](val input:HasVal[X], val bucketGen: () => Y, val env:Environment) {
  private var sliceTrigger :EventGraphObject = _
  private val sliceBefore = true

  def last() = {
    val slicer = new SlicedReduce[X, Y](input, sliceTrigger, sliceBefore, bucketGen, ReduceType.LAST, env)
    new MacroTerm[Y#OUT](env)(slicer)
  }

  // NODEPLOY - do the same delegation trick as GroupedTerm
  def all() = {
    val slicer = new SlicedReduce[X, Y](input, sliceTrigger, sliceBefore, bucketGen, ReduceType.CUMULATIVE, env)
    new MacroTerm[Y#OUT](env)(slicer)
  }

//  def every(macroTerm:MacroTerm[_], reset:SliceAlign = AFTER) : MacroTerm[Y#OUT] = {
//    every[MacroTerm[_]](macroTerm, reset = AFTER)(new MacroTermSliceTrigger)
//  }
//
  def every[S](sliceSpec:S, reset:SliceAlign = AFTER)(implicit ev:SliceTriggerSpec[S]) : GroupedTerm[X, Y] = {
//  def every[S : SliceTriggerSpec](sliceSpec:S, reset:SliceAlign = AFTER) : MacroTerm[Y#OUT] = {
//    val sliceTrigger = ev.buildTrigger(sliceSpec, input.getTrigger, env)
    val sliceTrigger = ev.buildTrigger(sliceSpec, input.getTrigger, env)
    val sliceBefore = reset == BEFORE
    val partialSlicer = (reduceType:ReduceType) => new SlicedReduce[X, Y](input, sliceTrigger, sliceBefore, bucketGen, reduceType, env)
    new GroupedTerm[X, Y](partialSlicer, env)(input)
  }

  def bind[S](stream:HasVal[S])(adder:X => S => Unit) :PartialAggOrAcc[X,Y] = ???
}

object MacroTerm {
  implicit def termToHasVal[E](term:MacroTerm[E]) :HasVal[E] = term.input
//  implicit def intToEvents(i:Int) = { Events(i) }
}

trait SliceTriggerSpec[-X] {
  def buildTrigger(x:X, src:EventGraphObject, env:types.Env) :types.EventGraphObject
}

object SliceTriggerSpec {
  implicit object DurationIsTriggerSpec extends SliceTriggerSpec[Duration] {
    def buildTrigger(duration:Duration, src: EventGraphObject, env: types.Env) = {
      new Timer(duration)
    }
  }
  implicit object EventsIsTriggerSpec extends SliceTriggerSpec[Events] {
    def buildTrigger(events:Events, src: EventGraphObject, env: types.Env) = {
      new NthEvent(events.n, src, env)    }
  }
  implicit object MacroIsTriggerSpec extends SliceTriggerSpec[MacroTerm[_]] {
    def buildTrigger(events:MacroTerm[_], src: EventGraphObject, env: types.Env) = {
      events.input.getTrigger
    }
  }
  implicit class MacroTermSliceTrigger[T](t:MacroTerm[T]) extends SliceTriggerSpec[MacroTerm[T]] {
    override def buildTrigger(x: MacroTerm[T], src: EventGraphObject, env: _root_.scespet.core.types.Env): _root_.scespet.core.types.EventGraphObject =  x.getTrigger
  }

  implicit def toTriggerSpec[T](m:MacroTerm[T]) :SliceTriggerSpec[MacroTerm[T]] = new MacroTermSliceTrigger[T](m)
}

