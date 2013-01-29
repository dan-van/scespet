package macrotest

import macros.{Foo, Reflect1}

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 09/01/2013
 * Time: 23:02
 * To change this template use File | Settings | File Templates.
 */
object TestMacro extends App {
  var i:Int = 1
  var reflect = new Reflect1("Hello")
  reflect.bar()
  var lift = reflect.danLift(newFoo)

  def newFoo: Foo = {
    i += 1
    new Foo("AFoo"+i)
  }

  var foo1 = lift.apply()
  var foo2 = lift.apply()
}
