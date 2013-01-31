package programs

import macros.{MacroApply, Builder, Wrapper, SimpleMacro}
import scespet.core.BucketBuilder

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 30/01/2013
 * Time: 10:59
 * To change this template use File | Settings | File Templates.
 */
object MacroCompileError extends App {
  val m: MacroApply[Int] = new MacroApply[Int](123)
  println(m.doMacro("foo").doMacro(1.23))
//  class Foo[T](var myX:String) extends Builder {
//
//   import language.experimental.macros
//
//    def build[X, TX](x:X, tx:TX):Pair[X,TX] = Pair(x, tx)
//
//    def doMacro[T](x:T):Pair[T,Foo] = macro SimpleMacro.returnX[T,Foo]
//  }

//  val foo: Foo = new Foo("Hello")
//  val someFoo: Option[Foo] = Some(foo)
//  val bb:BucketBuilder[Option[Foo]] = new BucketBuilder[Option[Foo]] {
//    def each(n: Int) = someFoo
//  }
//  println(foo.myX)

//  case class Bar(var str:String = "Bar")
//
//  val out: (Bar, Foo) = foo.doMacro(new Bar)
//  println(out._1)
//  println(out._2)
}
