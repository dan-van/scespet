package scespet.core

import scespet.core._
import reflect.macros.Context
import scala.reflect.ClassTag
import gsa.esg.mekon.core.EventGraphObject
import gsa.esg.mekon.core.EventGraphObject.Lifecycle


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
  def fold_all[Y <: Reduce[X]](y: Y):MacroTerm[Y] = {
    val listener = new AbsFunc[X,Y] {
      value = y
      initialised = true
      def calculate() = {
        y.add(input.value);
        true
      }
    }
    env.addListener(input.trigger, listener)
    return new MacroTerm[Y](env)(listener)
  }

  /**
   * Fold is a running reduction - the new state of the reduction is exposed after each element addition
   * e.g. a running cumulative sum, as opposed to a sum
   * @param y
   * @tparam Y
   * @return
   */
  def reduce_all[Y <: Reduce[X]](y: Y):MacroTerm[Y] = {
    val listener = new AbsFunc[X,Y] {
      val reduction = y
      value = null.asInstanceOf[Y]

      val termination = env.getTerminationEvent()
      env.addListener(termination, this)

      def calculate() = {
        if (env.hasChanged(termination)) {
          value = reduction
          initialised = true
          true
        } else {
          y.add(input.value)
          false
        }
      }
    }
    env.addListener(input.trigger, listener)
    return new MacroTerm[Y](env)(listener)
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

  def reduce[Y <: Reduce[X]](newBFunc: => Y):BucketBuilder[X, Y] = new BucketBuilderImpl[X,Y](() => newBFunc, MacroTerm.this, ReduceType.LAST, env)

  def fold[Y <: Reduce[X]](newBFunc: => Y):BucketBuilder[X, Y] = new BucketBuilderImpl[X,Y](() => newBFunc, MacroTerm.this, ReduceType.CUMULATIVE, env)
}
object MacroTerm {
  implicit def termToHasVal[E](term:MacroTerm[E]) :HasVal[E] = term.input
}