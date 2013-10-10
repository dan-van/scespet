package typetests

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 10/10/2013
 * Time: 16:43
 * To change this template use File | Settings | File Templates.
 */
object ChainedTypes2 extends App {
  abstract class Common[+X](val value:X) {
    def builder():Builder
  }

  class BuilderA extends Builder {
    def build[Y](y: Y): A[Y] = new A(y)
  }

  class BuilderB extends Builder {
    def build[Y](y: Y): B[Y] = new B(y)
  }

  class A[X](x:X) extends Common[X](x) {
    def doA() {
      println("done A")
    }
    def builder():BuilderA = new BuilderA()
  }

  class B[X](x:X) extends Common[X](x) {
    def doB() {
      println("done B")
    }

    def builder():BuilderB = new BuilderB()
  }

  trait Builder {
    def build[Y](y:Y) :Common[Y]
  }

  val a = new A("a")
  a.builder().build("a2").builder().build("a3").doA()

  val b = new B(1)
  b.builder().build(2).doB
}
