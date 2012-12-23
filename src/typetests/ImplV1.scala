package typetests

import typetests.Chaining.{Reduce, Term}
import scala.tools.reflect.ToolBox
import scala.reflect.runtime.{universe => ru}
import reflect.macros.Context

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 13/12/2012
 * Time: 11:04
 * To change this template use File | Settings | File Templates.
 */
object ImplV1 {

  class MyThing(var x:Int)

  object TupleExample {
    import scala.language.experimental.macros
    def fill[A](arity: Int)(a: A): Product = macro fill_impl[A]

    def fill_impl[A](c: Context)(arity: c.Expr[Int])(a: c.Expr[A]) = {
      import c.universe._

      arity.tree match {
        case Literal(Constant(n: Int)) if n < 23 => c.Expr(
          Apply(
            Select(Ident("Tuple" + n.toString), "apply"),
            List.fill(n)(a.tree)
          )
        )
        case _ => c.abort(
          c.enclosingPosition,
          "Desired arity must be a compile-time constant less than 23!"
        )
      }
    }
  }
}
