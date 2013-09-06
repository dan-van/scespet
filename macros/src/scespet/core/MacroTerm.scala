package scespet.core

import scespet.core._
import reflect.macros.Context
import scala.reflect.ClassTag


/**
 * This wraps an input HasVal with API to provide typesafe reactive expression building
 */
class MacroTerm[X](val env:types.Env)(val input:HasVal[X]) extends BucketTerm[X] {
  import scala.reflect.macros.Context
  import scala.language.experimental.macros
  import scala.collection.JavaConverters._


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
      def calculate() = {
        y.add(input.value);
        true
      }
    }
    env.addListener(input.trigger, listener)
    return new MacroTerm[Y](env)(listener)
  }

  def map[Y](f: (X) => Y):MacroTerm[Y] = {
    val listener = new AbsFunc[X,Y] {
      def calculate() = {
        value = f(input.value);
        true
      }
    }
    env.addListener(input.trigger, listener)
    return new MacroTerm[Y](env)(listener)
  }

  def filterType[Y : ClassTag]():MacroTerm[Y] = {
    filter(_.isInstanceOf[Y]).map(_.asInstanceOf[Y])
  }

  def filter(accept: (X) => Boolean):MacroTerm[X] = {
    class FilteredValue extends UpdatingHasVal[X] {
      var value = null.asInstanceOf[X]

      def calculate() = {
        if (accept(input.value)) {
          value = input.value
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
    env.addListener(input.trigger, vFunc)
    return new VectTerm[K, X](env)(vFunc)
  }

  def takef[Y](newGenerator:(X)=>HasVal[Y]) : VectTerm[X,Y] = {
    this.by(x => x).joinf(newGenerator)
  }

// Take a stream of X (usually representing some collection), map X -> Collection[Y] and generate a new flattened set of Y (mainly used if X is some form of collection and you want to flatten it)
//  def values[X1 <: ClassTag[X]]() : VectTerm[X1,X1] = values[X1](x => Traversable(x.asInstanceOf[X1]))

//  def distinct2[Y : ClassTag]() = values[Y](x => {Traversable[Y](x.asInstanceOf[Y])} )

  private def valueToSingleton[Y] = (x:X) => Traversable(x.asInstanceOf[Y])

  def valueSet() : VectTerm[X,X] = valueSet(valueToSingleton[X])

  /**
   * generate a new vector, keyed by the values in this stream.
   * optionally provide a function that takes a value in the stream and yields an iterator to be used for generating many keys from one value
   * (this is especially useful if your values in the stream are Collections, as this allows you to flatten a Stream[Set[String]] -> Vect[String,String]
   *
   * @param expand
   * @tparam Y
   * @return
   */
  def valueSet[Y](expand: (X=>TraversableOnce[Y]) = valueToSingleton[X] ) : VectTerm[Y,Y] = {
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

  def join[Y](y:MacroTerm[Y]):MacroTerm[(X,Y)] = {
    val listener = new UpdatingHasVal[(X,Y)] {
      var value:(X,Y) = _

      def calculate() = {
        var aY:Y = y.input.value
        value = (input.value, aY)
        true
      }
    }
    env.addListener(input.trigger, listener)
    env.addListener(y.input.trigger, listener)
    return new MacroTerm[(X,Y)](env)(listener)
  }

  def take[Y](y:MacroTerm[Y]):MacroTerm[(X,Y)] = {
    val listener = new UpdatingHasVal[(X,Y)] {
      var value:(X,Y) = _

      def calculate() = {
        value = (input.value, y.input.value)
        true
      }
    }
    env.addListener(input.trigger, listener)
    return new MacroTerm[(X,Y)](env)(listener)
  }

//  def reduce[Y <: Reduce[X]](bucketFunc: Y, window: Window):Term[Y] = macro BucketMacro.reduce[X,Y]

//  def reduce[Y <: Reduce[X]](bucketFunc: Y):BucketBuilder[MacroTerm[Y]] = macro BucketMacro.bucket2Macro[MacroTerm[Y],Y]
  def reduce[Y](bucketFunc: Y):BucketBuilder[X,Y] = macro BucketMacro.bucket2Macro[X,Y]

  /**
   * This yields a partially built reduction, the next call determines when the reduction terminates (and yields a result)
   * e.g. reduce( new Sum ) each (10)
   * will yield the sum of every 10 elements
   * @param newBFunc
   * @tparam Y
   * @return
   */
  def bucket2NoMacro[Y <: Reduce[X]](newBFunc:() => Y):BucketBuilder[X, Y] = new BucketBuilderImpl[X,Y](newBFunc, MacroTerm.this, env)

  def newBucketBuilder[B](newB: () => B):BucketBuilder[X, B] = {
    type Y = B with Reduce[X]
    val reduceGenerator = newB.asInstanceOf[() => Y]
//    val reduceGenerator = newB
    var aY = reduceGenerator.apply()
    println("Reducer in MacroTerm generates "+aY)
//    new BucketBuilderImpl[X, B](reduceGenerator, input, eval).asInstanceOf[BucketBuilder[T]]
    new BucketBuilderImpl[X, Y](reduceGenerator, new MacroTerm[X](env)(input), env).asInstanceOf[BucketBuilder[X, B]]
  }
}
