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
    this.by(x => x).mapk(newGenerator)
  }

// Take a stream of X (usually representing some collection), map X -> Collection[Y] and generate a new flattened set of Y (mainly used if X is some form of collection and you want to flatten it)
//  def values[X1 <: ClassTag[X]]() : VectTerm[X1,X1] = values[X1](x => Traversable(x.asInstanceOf[X1]))

//  def distinct2[Y : ClassTag]() = values[Y](x => {Traversable[Y](x.asInstanceOf[Y])} )

  def valueSet[Y : ClassTag](expand: (X=>TraversableOnce[Y]) = ( (x:X) => Traversable(x.asInstanceOf[Y]) ) ) : VectTerm[Y,Y] = {
    val initial = expand(input.value)
    val typeY = reflect.classTag[Y]
    val javaClass : Class[Y] = typeY.runtimeClass.asInstanceOf[Class[Y]]
    val flattenedSet = new MutableVector[Y](javaClass, initial.toIterable.asJava, env) with types.MFunc {
      env.addListener(input.trigger, this)

      def calculate(): Boolean = {
        var toAdd = expand(input.value)
        toAdd.foreach( add(_) )
        false
      }
    }
    return new VectTerm[Y, Y](env)(flattenedSet)
  }

  def join[Y](y:MacroTerm[Y]):MacroTerm[(X,Y)] = {
    val listener = new HasVal[(X,Y)] with types.MFunc {
      def trigger = this

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
    val listener = new HasVal[(X,Y)] with types.MFunc {
      def trigger = this

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
