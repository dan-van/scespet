package macrotest

import reflect.ClassTag

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 10/01/2013
 * Time: 23:48
 * To change this template use File | Settings | File Templates.
 */
object MacroTest {
  import reflect.macros.Context
  import scala.language.experimental.macros

  trait ResultBase[X] {
    def doStuff:X
  }

  trait MyIntermediate[T] {
    def doCollapse():T
  }

  class ClassFoo[X :ClassTag](val x:X) {
    //        def bucket2[Y <: Reduce[X]](y:Y):BucketBuilder[Term[Y]] = ???
    //    def generateIntermediate[B <: ResultBase[X]](myOutput:B):MyIntermediate[ClassFoo[B]] = macro macroImpl2[X, B]
    def generateIntermediate2[B <: ResultBase[X]](myOutput:B):MyIntermediate[B] = macro macroImpl3[X, B]
  }

  //  def macroImpl1[X:c.WeakTypeTag, B:c.WeakTypeTag](c :Context)(myOutput:c.Expr[B]):c.Expr[MyIntermediate[X,B]] = {
  //    import c.universe._
  //    reify {
  //      new MyIntermediate[X, B] {def doCollapse = myOutput.splice}
  //    }
  //  }

  def macroImpl2[X:c.WeakTypeTag, B <: ResultBase[X] : ClassTag](c :Context)(myOutput:c.Expr[B]):c.Expr[MyIntermediate[ClassFoo[B]]] = {
    import c.universe._
    reify {
      new MyIntermediate[ClassFoo[B]] {def doCollapse = new ClassFoo[B](myOutput.splice)}
    }
  }

  def macroImpl3[X:c.WeakTypeTag, B : c.WeakTypeTag](c :Context)(myOutput:c.Expr[B]):c.Expr[MyIntermediate[B]] = {
    import c.universe._
    reify {
      type B_ = B with ResultBase[X]
      new MyIntermediate[B_] {def doCollapse = myOutput.splice.asInstanceOf[B_]}.asInstanceOf[MyIntermediate[B]]
    }
  }
}
