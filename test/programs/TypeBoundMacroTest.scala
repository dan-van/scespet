package programs


/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 10/01/2013
 * Time: 23:47
 * To change this template use File | Settings | File Templates.
 */

object TypeBoundMacroTest extends App {
  import macros.TypeBoundMacro.{_}
  class MyResult() extends ResultBase[String] {
    def doStuff = ???
    def doThing(t:String) = println(this+" got a t="+t)
    def otherStuff = "goodbye instance " + this
  }

  var start = new ClassFoo[String]("START")
  var builder = start.generateIntermediate2(new MyResult).doCollapse()
  var result: MyResult = builder
  println(result)

//  builde(new ResultBase[MyResult] {
//    def doStuff = ???
//  })

//  var builder = start.generateIntermediate(new MyResult).doCollapse()
//  var result: MyResult = builder.x
//  println(result)
//
//  builder.generateIntermediate(new ResultBase[MyResult] {
//    def doStuff = ???
//  })
}

