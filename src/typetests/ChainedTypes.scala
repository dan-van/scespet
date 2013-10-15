package typetests

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 10/10/2013
 * Time: 16:43
 * To change this template use File | Settings | File Templates.
 */
//object ChainedTypes extends App {
//  abstract class Common[+X](val value:X) {
//    def builder():Builder
//  }
//
//  class A[X](x:X) extends Common[X](x) {
//    def doA() {
//      println("done A")
//    }
//    def builder() = new Builder {
//      override type T[x] = A[x]
//      def build[Y](y: Y): this.type#T[Y] = new A(y)
//    }
//  }
//
//  class B[X](x:X) extends Common[X](x) {
//    def doB() {
//      println("done B")
//    }
//
//    def builder() = new Builder {
//      override type T[x] = B[x]
//      def build[Y](y: Y): this.type#T[Y] = new B(y)
//    }
//  }
//
//  trait Builder {
//    type T[x] = Common[x]
//    def build[Y](y:Y) :T[Y]
//  }
//
//  val a = new A("a")
//  a.builder().build("a2").doA
//
//  val b = new B(1)
//  b.builder().build(2).doB
//}
