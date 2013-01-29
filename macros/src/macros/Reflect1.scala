package macros

import reflect.macros.Context
import scala.language.experimental.macros
import scala.tools.reflect.Eval
import com.sun.xml.internal.fastinfoset.tools.XML_DOM_FI


/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 08/12/2012
 * Time: 23:44
 * To change this template use File | Settings | File Templates.
 */
class Reflect1(val myStr:String) {
  def bar() {println("Buggrit")}
  def danLift[X <: Foo](x:X): () => X = macro Reflect1.danLiftMacro[X]
  def foo():String = {println(myStr);return myStr}
}


class Foo(val s:String) {
  override def toString = "Foo(" + s + ")"
}

object Reflect1 {

//  def newBuild[X](x:X): () => Foo = macro buildF[X]
//  def buildF[X](c: Context)(x: c.Expr[X]): c.Expr[() => Foo] = {
//    import c.universe._
//    reify(() => {val foo = new Foo("test"); println("applying build "+foo); foo})
//  }
  def danLiftMacro[X <: Foo : c.WeakTypeTag](c: Context{type PrefixType = Reflect1})(x: c.Expr[X]) : c.Expr[() => X] = {
    import c.universe._
    reify{
      val singleF:X = x.splice;
      () => {
        println("Building a Foo inst from: "+c.prefix.splice)
//        var f:X = x.splice
        var f:X = singleF
        println("Foo instance is: "+f+" with sValue: "+f.s)
        f
      }
    }
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
  /*
  var reflect = new Reflect1("Hello")
  var lift = reflect.danLift(new Foo("AFoo"))
  var foo1 = lift.apply()
  var foo2 = lift.apply()
  */
}

