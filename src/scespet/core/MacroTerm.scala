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
import scespet.core.types.Events


/**
 * This wraps an input HasVal with API to provide typesafe reactive expression building
 */
class MacroTerm[X](val env:types.Env)(val input:HasVal[X]) extends Term[X] with HasVal[X] {
  import scala.language.experimental.macros
  import scala.collection.JavaConverters._

//  override implicit def toHasVal():HasVal[X] = input
  override def value = input.value
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
  def scan[Y <: Agg[X]](newBFunc: => Y) = group[Null](null, AFTER)(SliceTriggerSpec.NULL).scan(newBFunc)

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
    new GroupedTerm(uncollapsed, env)
  }

  // NODEPLOY rename SliceAlign to TriggerAlign, with values = OPEN/CLOSE
  def group[S](sliceSpec:S, triggerAlign:SliceAlign = AFTER)(implicit ev:SliceTriggerSpec[S]) :GroupedTerm[X] = {
    val uncollapsed = new UncollapsedGroupWithTrigger[S, X](input, sliceSpec, triggerAlign, env, ev)
    new GroupedTerm[X](uncollapsed, env)
  }
}

//type UncollapsedGroup[C <: Cell] = (C) => C#OUT
trait UncollapsedGroup[IN] {
  def applyB[B <: Bucket](lifecycle:SliceCellLifecycle[B], reduceType:ReduceType) :HasVal[B#OUT]
  def applyAgg[A <: Agg[IN]](lifecycle:SliceCellLifecycle[A], reduceType:ReduceType) :HasVal[A#OUT]
}

class UncollapsedGroupWithTrigger[S, IN](input:HasValue[IN], sliceSpec:S, triggerAlign:SliceAlign, env:types.Env, ev: SliceTriggerSpec[S]) extends UncollapsedGroup[IN] {
  def applyAgg[A <: Agg[IN]](lifecycle: SliceCellLifecycle[A], reduceType:ReduceType): HasVal[A#OUT] = {
    new SlicedReduce[S, IN, A](input, sliceSpec, triggerAlign == BEFORE, lifecycle, reduceType, env, ev)
  }

  def applyB[B <: Bucket](lifecycle: SliceCellLifecycle[B], reduceType:ReduceType): HasVal[B#OUT] = {
    triggerAlign match {
      case BEFORE => new SliceBeforeBucket[S, B](sliceSpec, lifecycle, reduceType, env, ev)
      case AFTER => new SliceAfterBucket[S, B](sliceSpec, lifecycle, reduceType, env, ev)
      case _ => throw new IllegalArgumentException(String.valueOf(triggerAlign))
    }
  }
}

class GroupedTerm[X](val uncollapsedGroup: UncollapsedGroup[X], val env:types.Env) {
//  def reduce[Y <: Cell](newBFunc: => Y) :Term[Y#OUT] = ???
//  def reduce[Y <: Cell](newBFunc: => Y)(implicit ev:Y <:< Agg[X]) :Term[Y#OUT] = {
  def reduce[Y <: Cell](newBFunc: => Y)(implicit ev:Y <:< Agg[X]) :Term[Y#OUT] = {
    // damn, my implicit evidence doesn't seem to help the compiler realise that Y <: Agg[X]. Hack time.
    type A = Y with Agg[X]
    // NODEPLOY - if I make a mutable Cell[X] which delegates to an instance of Agg[X], then I can implement reset
    val lifecycle :SliceCellLifecycle[Y] = new SliceCellLifecycle[Y] {
      override def newCell(): Y = newBFunc
      override def closeCell(c: Y): Unit = {}
      override def reset(c: Y): Unit = {}
    }
    val slicer :HasVal[Y#OUT] = uncollapsedGroup.applyAgg[A](lifecycle.asInstanceOf[SliceCellLifecycle[A]], ReduceType.LAST).asInstanceOf[HasVal[Y#OUT]]
    new MacroTerm[Y#OUT](env)(slicer)
  }

  // NODEPLOY bring this up to Cell
  def scan[Y <: Agg[X]](newBFunc: => Y)(implicit ev:Y <:< Agg[X]) :Term[Y#OUT] = {
    // damn, my implicit evidence doesn't seem to help the compiler realise that Y <: Agg[X]. Hack time.
    // NODEPLOY - if I make a mutable Cell[X] which delegates to an instance of Agg[X], then I can implement reset
//    val lifecycle = new SliceCellLifecycle[CellFromAgg[Y]] {
//      override def newCell(): CellFromAgg[Y] = {
//        val cell = new CellFromAgg[Y]
//        reset(cell)
//        cell
//      }
//      override def closeCell(c: CellFromAgg[Y]): Unit = c.complete()
//      override def reset(c: CellFromAgg[Y]): Unit = {
//        c.agg = newBFunc.asInstanceOf[Y]
//      }
//    }
    val lifecycle = new SliceCellLifecycle[Y] {
      override def newCell(): Y = newBFunc

      override def closeCell(c: Y): Unit = {}

      override def reset(c: Y): Unit = {}
    }
//    val slicer :HasVal[Y#OUT] = uncollapsedGroup.applyAgg[A](lifecycle.asInstanceOf[SliceCellLifecycle[A]], ReduceType.CUMULATIVE).asInstanceOf[HasVal[Y#OUT]]
    val slicer :HasVal[Y#OUT] = uncollapsedGroup.applyAgg(lifecycle, ReduceType.CUMULATIVE).asInstanceOf[HasVal[Y#OUT]]
    new MacroTerm[Y#OUT](env)(slicer)
  }
}

// NODEPLOY this should be a Term that also supports partitioning operations
class PartialBuiltSlicedBucket[Y <: Bucket](val cellLifecycle: SliceCellLifecycle[Y], val env:Environment) {
  var bindings = List[(HasVal[_], (_ => _ => Unit))]()

  private lazy val scanAllTerm: MacroTerm[Y#OUT] = {
    val slicer = new SliceAfterBucket[Null, Y](null, cellLifecycle, ReduceType.CUMULATIVE, env, SliceTriggerSpec.NULL)
    // add the captured bindings
    bindings.foreach(pair => {
      val (hasVal, adder) = pair
      type IN = Any
      slicer.addInputBinding[IN](hasVal.asInstanceOf[HasVal[IN]], adder.asInstanceOf[Y => IN => Unit])
    })
    new MacroTerm[Y#OUT](env)(slicer)
  }


  def last(): MacroTerm[Y#OUT] = {
    val slicer = new SliceBeforeBucket[Any, Y](null, cellLifecycle, ReduceType.LAST, env, SliceTriggerSpec.TERMINATION)
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

  // NODEPLOY - I think this would be better named as 'reset', once you already have a stream->reducer binding, talking about grouping is confusing.
  //NODEPLOY - think:
  // CellLifecycle creates a new cell at beginning of stream, then multiple calls to close bucket after a slice
  // this avoids needing a new slice trigger definition each slice.
  def group[S](sliceSpec: S, triggerAlign: SliceAlign = AFTER)(implicit ev: SliceTriggerSpec[S]):PartialGroupedBucketStream[S, Y] = {
    new PartialGroupedBucketStream[S, Y](triggerAlign, cellLifecycle, bindings, sliceSpec, ev, env)
  }
}

object MacroTerm {
  implicit def termToHasVal[E](term:MacroTerm[E]) :HasVal[E] = term.input
//  implicit def intToEvents(i:Int) = { Events(i) }
}

trait SliceTriggerSpec[-X] {
  // NODEPLOU - can I delete src?
  def buildTrigger(x:X, src:Set[EventGraphObject], env:types.Env) :types.EventGraphObject
}

object SliceTriggerSpec {
  val TERMINATION = new SliceTriggerSpec[Any] {
    def buildTrigger(x:Any, src: Set[EventGraphObject], env: types.Env) = {
      env.getTerminationEvent
    }
  }
  val NULL = new SliceTriggerSpec[Null] {
    def buildTrigger(x:Null, src: Set[EventGraphObject], env: types.Env) = {
      null
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

