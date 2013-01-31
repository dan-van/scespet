package programs

import macros.{Builder, Wrapper, SimpleMacro}
import scespet.core.BucketBuilder

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 30/01/2013
 * Time: 10:59
 * To change this template use File | Settings | File Templates.
 */
object MacroCompileError extends App {
  class Foo(var myX:String) extends Builder {

   import language.experimental.macros

    def build[B]():Wrapper[B] = new Wrapper[B] {
      def getX = {
        // THIS IS OBVIOUSLY WRONG
        this.asInstanceOf[B]
      }
    }

    def doMacro[T](x:T):Wrapper[T] = macro SimpleMacro.returnX[T,Wrapper[T]]
  }

  val foo: Foo = new Foo("Hello")
  val someFoo: Option[Foo] = Some(foo)
  val bb:BucketBuilder[Option[Foo]] = new BucketBuilder[Option[Foo]] {
    def each(n: Int) = someFoo
  }
  println(foo.doMacro(foo).getX.myX)
}
