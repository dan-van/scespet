package macros

import reflect.macros.Context
import scala.language.experimental.macros


/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 08/12/2012
 * Time: 23:44
 * To change this template use File | Settings | File Templates.
 */
object Reflect1 {
  class Foo() {
    override def toString = super.toString
  }

  def newBuild[X](x:X): () => Foo = macro buildF[X]
  def buildF[X](c: Context)(x: c.Expr[X]): c.Expr[() => Foo] = {
    import c.universe._
    reify(() => {val foo = new Foo; println("applying build "+foo); foo})
  }

  def danLift[X](x:X): () => X = macro danLiftMacro[X]
  def danLiftMacro[X](c: Context)(x: c.Expr[X]): c.Expr[() => X] = {
    import c.universe._
    reify(() => {val foo:X = x.splice; println("constructed new value: "+foo); foo})
  }

  //  class Queryable[T, Repr](query: Query) {
//    def filter(p: T => Boolean): Repr = macro Impl.filter[T, Repr]
//  }
//
//  object Impl {
//    def filter[T: c.TypeTag, Repr: c.TypeTag](c: Context)(p: c.Expr[T => Boolean]) = reify {
//      val b = c.prefix.value.newBuilder
//      b.query = Filter(c.prefix.value.query, c.reifyTree(p))
//      b.result
//    }
//  }
}
