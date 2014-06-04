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
import scespet.core.types.{MFunc, Events}
import scespet.core.types.Events
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

  // THINK: this could be special cased to be faster
  // NODEPLOY rename to reduce
  def reduce[Y <: Agg[X]](newBFunc: => Y):Term[Y#OUT] = group[Any](null, AFTER)(SliceTriggerSpec.TERMINATION).reduce(newBFunc)

  // THINK: this could be special cased to be faster
  def scan[Y <: Agg[X]](newBFunc: => Y) = group[Any](null, AFTER)(SliceTriggerSpec.TERMINATION).scan(newBFunc)

  def window(window:HasValue[Boolean]) : GroupedTerm[X] = {
    val uncollapsed = new UncollapsedGroup[X] {
      def applyB[B <: Bucket](lifecycle: SliceCellLifecycle[B], reduceType: ReduceType): HasVal[B#OUT] = {
        reduceType match {
          case ReduceType.CUMULATIVE => new WindowedBucket_Continuous[B](window, lifecycle, env)
          case ReduceType.LAST => new WindowedBucket_LastValue[B](window, lifecycle, env)
        }
      }

      def applyAgg[A <: Agg[X]](lifecycle: SliceCellLifecycle[A], reduceType: ReduceType): HasVal[A#OUT] = {
        new WindowedReduce[X, A](input, window, lifecycle, reduceType, env)
      }
    }
    new GroupedTerm[X](input, uncollapsed, env)
  }

  // NODEPLOY rename SliceAlign to TriggerAlign, with values = OPEN/CLOSE
  def group[S](sliceSpec:S, triggerAlign:SliceAlign = AFTER)(implicit ev:SliceTriggerSpec[S]) :GroupedTerm[X] = {
    val sliceTrigger = ev.buildTrigger(sliceSpec, Set(input.getTrigger), env)
    val uncollapsed = new UncollapsedGroup[X] {
      def applyAgg[A <: Agg[X]](lifecycle: SliceCellLifecycle[A], reduceType:ReduceType): HasVal[A#OUT] = {
        new SlicedReduce[X, A](input, sliceTrigger, triggerAlign == BEFORE, lifecycle, reduceType, env)
      }

      def applyB[B <: Bucket](lifecycle: SliceCellLifecycle[B], reduceType:ReduceType): HasVal[B#OUT] = {
        triggerAlign match {
          case BEFORE => new SliceBeforeBucket[B](sliceTrigger, lifecycle, reduceType, env)
          case AFTER => new SliceAfterBucket[B](sliceTrigger, lifecycle, reduceType, env)
          case _ => throw new IllegalArgumentException(String.valueOf(triggerAlign))
        }
      }
    }
    new GroupedTerm[X](input, uncollapsed, env)
  }
}

//type UncollapsedGroup[C <: Cell] = (C) => C#OUT
trait UncollapsedGroup[IN] {
  def applyB[B <: Bucket](lifecycle:SliceCellLifecycle[B], reduceType:ReduceType) :HasVal[B#OUT]
  def applyAgg[A <: Agg[IN]](lifecycle:SliceCellLifecycle[A], reduceType:ReduceType) :HasVal[A#OUT]

}

class GroupedTerm[X](val input:HasVal[X], val uncollapsedGroup: UncollapsedGroup[X], val env:types.Env) {
  def reduce[Y <: Agg[X]](newBFunc: => Y) :Term[Y#OUT] = {
    val lifecycle = new SliceCellLifecycle[Y] {
      override def newCell(): Y = newBFunc
      override def closeCell(c: Y): Unit = c.complete()
    }
    val slicer = uncollapsedGroup.applyAgg(lifecycle, ReduceType.LAST)
    new MacroTerm[Y#OUT](env)(slicer)
  }

  def scan[Y <: Agg[X]](newBFunc: => Y) :Term[Y#OUT] = {
    val lifecycle = new SliceCellLifecycle[Y] {
      override def newCell(): Y = newBFunc
      override def closeCell(c: Y): Unit = c.complete()
    }
    val slicer = uncollapsedGroup.applyAgg(lifecycle, ReduceType.CUMULATIVE)
    new MacroTerm[Y#OUT](env)(slicer)
  }
}

class GroupedBucketTerm[C <: Bucket](reduceBuilder:(ReduceType)=>SlicedBucket[C], val env:types.Env) {
  private lazy val scanTerm :MacroTerm[C#OUT] = {
    val slicer = reduceBuilder(ReduceType.CUMULATIVE)
    new MacroTerm[C#OUT](env)(slicer)
  }

  def last() = {
    val slicer = reduceBuilder(ReduceType.LAST)
    new MacroTerm[C#OUT](env)(slicer)
  }

  // NODEPLOY - evaporate this method by using delegation onto lazyVal
  def all() = scanTerm
}

// NODEPLOY this should be a Term that also supports partitioning operations
class PartialBuiltSlicedBucket[Y <: Bucket](val cellLifecycle: SliceCellLifecycle[Y], val env:Environment) {
  var bindings = List[(HasVal[_], (_ => _ => Unit))]()

  private lazy val scanAllTerm: MacroTerm[Y#OUT] = {
    val slicer = new SliceBeforeBucket[Y](env.getTerminationEvent, cellLifecycle, ReduceType.CUMULATIVE, env)
    // add the captured bindings
    bindings.foreach(pair => {
      val (hasVal, adder) = pair
      type IN = Any
      slicer.addInputBinding[IN](hasVal.asInstanceOf[HasVal[IN]], adder.asInstanceOf[Y => IN => Unit])
    })
    new MacroTerm[Y#OUT](env)(slicer)
  }


  def last(): MacroTerm[Y#OUT] = {
    val slicer = new SliceBeforeBucket[Y](env.getTerminationEvent, cellLifecycle, ReduceType.LAST, env)
    // add the captured bindings
    bindings.foreach(pair => {
      val (hasVal, adder) = pair
      type IN = Any
      slicer.addInputBinding[IN](hasVal.asInstanceOf[HasVal[IN]], adder.asInstanceOf[Y => IN => Unit])
    })
    new MacroTerm[Y#OUT](env)(slicer)
  }

  // NODEPLOY - delegate remaining Term interface calls here using lazyVal approach
  def all(): MacroTerm[Y#OUT] = scanAllTerm

  def bind[S](stream: HasVal[S])(adder: Y => S => Unit): PartialBuiltSlicedBucket[Y] = {
    bindings :+=(stream, adder)
    this
  }

  def group[S](sliceSpec: S, triggerAlign: SliceAlign = AFTER)(implicit ev: SliceTriggerSpec[S]): GroupedBucketTerm[Y] = {
    // NODEPLOY - GroupedBucketTerm needs to provide reduce and scan operations
    ???
  }
}

object MacroTerm {
  implicit def termToHasVal[E](term:MacroTerm[E]) :HasVal[E] = term.input
//  implicit def intToEvents(i:Int) = { Events(i) }
}

trait SliceTriggerSpec[-X] {
  def buildTrigger(x:X, src:Set[EventGraphObject], env:types.Env) :types.EventGraphObject
}

object SliceTriggerSpec {
  val TERMINATION = new SliceTriggerSpec[Any] {
    def buildTrigger(x:Any, src: Set[EventGraphObject], env: types.Env) = {
      env.getTerminationEvent
    }
  }

  implicit object DurationIsTriggerSpec extends SliceTriggerSpec[Duration] {
    def buildTrigger(duration:Duration, src: Set[EventGraphObject], env: types.Env) = {
      new Timer(duration)
    }
  }
  implicit object EventsIsTriggerSpec extends SliceTriggerSpec[Events] {
    def buildTrigger(events:Events, src: Set[EventGraphObject], env: types.Env) = {
      new NthEvent(events.n, src, env)    }
  }
  implicit object EventObjIsTriggerSpec extends SliceTriggerSpec[EventGraphObject] {
    def buildTrigger(events:EventGraphObject, src: Set[EventGraphObject], env: types.Env) = {
      events
    }
  }
  // NODEPLOY one of these is unused
  implicit object MacroIsTriggerSpec extends SliceTriggerSpec[MacroTerm[_]] {
    def buildTrigger(events:MacroTerm[_], src: Set[EventGraphObject], env: types.Env) = {
      events.input.getTrigger
    }
  }
  // NODEPLOY one of these is unused
  implicit class MacroTermSliceTrigger[T](t:MacroTerm[T]) extends SliceTriggerSpec[MacroTerm[T]] {
    override def buildTrigger(x: MacroTerm[T], src: Set[EventGraphObject], env: _root_.scespet.core.types.Env): _root_.scespet.core.types.EventGraphObject =  x.getTrigger
  }

  implicit def toTriggerSpec[T](m:MacroTerm[T]) :SliceTriggerSpec[MacroTerm[T]] = new MacroTermSliceTrigger[T](m)
}

