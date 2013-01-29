package typetests

import scespet.core._

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 17/01/2013
 * Time: 21:47
 * To change this template use File | Settings | File Templates.
 */
class MacroTerm[X](val eval:FuncCollector)(val input:HasVal[X]) extends Term[X] with BucketTerm {
  import scala.reflect.macros.Context
  import scala.language.experimental.macros

  override def map[Y <: Reduce[X]](y: Y):Term[Y] = {
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

  override def map[Y](f: (X) => Y):Term[Y] = {
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
  override def by[K](f: X => K) : VectTerm[K,X] = {
    val vFunc: GroupFunc[K, X] = new GroupFunc[K, X](input, f, eval.env)
    eval.bind(input.trigger, vFunc)
    return new MacroVectTerm[K, X](eval)(vFunc)
}

//  def bucket[Y <: Reduce[X]](bucketFunc: Y, window: Window):Term[Y] = macro BucketMacro.bucket[X,Y]

  def bucketFoo[Y <: Reduce[X]](bucketFunc: Y):BucketBuilder[Any] = macro BucketMacro.bucket2Macro[Term[Y],Y]

  def bucket2NoMacro[Y <: Reduce[X]](newBFunc:() => Y):BucketBuilder[Term[Y]] = new BucketBuilderImpl[X,Y](newBFunc, input, eval)

  def newBucketBuilder[B, T](newB: () => B):BucketBuilder[T] = {
    type Y = B with Reduce[X]
    val reduceGenerator = newB.asInstanceOf[() => Y]
    var aY: Y = reduceGenerator.apply()
    println("Reducer in MacroTerm generates "+aY)
    new BucketBuilderImpl[X, Y](reduceGenerator, input, eval).asInstanceOf[BucketBuilder[T]]
  }
}
