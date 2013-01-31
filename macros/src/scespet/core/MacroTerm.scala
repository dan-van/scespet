package scespet.core

import scespet.core._
import reflect.macros.Context


/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 17/01/2013
 * Time: 21:47
 * To change this template use File | Settings | File Templates.
 */
class MacroTerm[X](val eval:FuncCollector)(val input:HasVal[X]) extends BucketTerm {
  import scala.reflect.macros.Context
  import scala.language.experimental.macros

  def map[Y <: Reduce[X]](y: Y):MacroTerm[Y] = {
    val listener = new AbsFunc[X,Y] {
      value = y
      def calculate() = {
        y.add(input.value);
        true
      }
    }
    eval.bind(input.trigger, listener)
    return new MacroTerm[Y](eval)(listener)
  }

  def map[Y](f: (X) => Y):MacroTerm[Y] = {
    val listener = new AbsFunc[X,Y] {
      def calculate() = {
        value = f(input.value);
        true
      }
    }
    eval.bind(input.trigger, listener)
    return new MacroTerm[Y](eval)(listener)
  }
//  override def by[K](f: X => K) : VectTerm[K,X] = macro ByMacro.by[K,X]
  def by[K](f: X => K) : VectTerm[K,X] = {
    val vFunc: GroupFunc[K, X] = new GroupFunc[K, X](input, f, eval.env)
    eval.bind(input.trigger, vFunc)
    return new VectTerm[K, X](eval)(vFunc)
}

//  def bucket[Y <: Reduce[X]](bucketFunc: Y, window: Window):Term[Y] = macro BucketMacro.bucket[X,Y]

//  def bucket[Y <: Reduce[X]](bucketFunc: Y):BucketBuilder[MacroTerm[Y]] = macro BucketMacro.bucket2Macro[MacroTerm[Y],Y]
  def bucket[Y](bucketFunc: Y):BucketBuilder[Y] = macro BucketMacro.bucket2Macro[Y]

  def bucket2NoMacro[Y <: Reduce[X]](newBFunc:() => Y):BucketBuilder[Y] = new BucketBuilderImpl[X,Y](newBFunc, input, eval)

  def newBucketBuilder[B](newB: () => B):BucketBuilder[B] = {
    type Y = B with Reduce[X]
    val reduceGenerator = newB.asInstanceOf[() => Y]
//    val reduceGenerator = newB
    var aY = reduceGenerator.apply()
    println("Reducer in MacroTerm generates "+aY)
//    new BucketBuilderImpl[X, B](reduceGenerator, input, eval).asInstanceOf[BucketBuilder[T]]
    new BucketBuilderImpl[X, Y](reduceGenerator, input, eval).asInstanceOf[BucketBuilder[B]]
  }
}
