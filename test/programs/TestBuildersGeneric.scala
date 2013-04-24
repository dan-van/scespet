package programs

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 22/04/2013
 * Time: 13:15
 * To change this template use File | Settings | File Templates.
 */
object TestBuildersGeneric {
  class Builder[T <: Term[X], X](val t:T) {
    def build[Y](y:Y) = t.newSelf(y)
  }

  abstract class Term[X](var x:X) {
    type SELF[Y] <: Term[Y]
    def newSelf[Y](y:Y):SELF[Y]
//    def newBuilder() :Builder[SELF[_]] = new Builder[SELF[_]](this.asInstanceOf[SELF[_]])
    def newBuilder() :Builder[SELF[X], X] = new Builder[SELF[X],X](newSelf[X](x))
  }

  class ConcreteTermFoo[X](x:X) extends Term[X](x) {
    override type SELF[Y] <: ConcreteTermFoo[Y]
    def foo = "foo"

    def newSelf[Y](y:Y): SELF[Y] = new ConcreteTermFoo[Y](y).asInstanceOf[SELF[Y]]
  }

  class ConcreteTermBar[X](x:X) extends Term[X](x) {
    override type SELF[Y] <: ConcreteTermBar[Y]
    def bar = "bar"

    def newSelf[Y](y:Y): SELF[Y] = new ConcreteTermBar(y).asInstanceOf[SELF[Y]]
  }

//  class ConcreteTermA() extends Term() {
//    override type SELF <: ConcreteTermA
//    def foo = "Foo"
//
//    def newSelf(): SELF = new ConcreteTermA().asInstanceOf[SELF]
//  }
//
  class ConcreteTermSubA[X](x:X) extends ConcreteTermFoo[X](x) {
    override type SELF[Y] <: ConcreteTermSubA[Y]
    def subfoo = "subFoo"

    override def foo: String = "overrideFoo"

    override def newSelf[Y](y: Y): SELF[Y] = new ConcreteTermSubA(y).asInstanceOf[SELF[Y]]
  }

  def main(args :Array[String] ) {
    println( new ConcreteTermFoo().newSelf().newSelf().foo )
    println( new ConcreteTermBar(10).newSelf("hello").newSelf(1.23).bar )

    println( new ConcreteTermFoo().newBuilder().build(10).newSelf().foo )
    println( new ConcreteTermBar(10).newBuilder().build("hello").newSelf(1.23).bar )

    println( new ConcreteTermSubA(10).newBuilder().build("hello").newSelf(1.23).foo )
    println( new ConcreteTermSubA(10).newBuilder().build("hello").newSelf(1.23).subfoo )
//    println( new ConcreteTermSubA().newSelf().newSelf().foo )
//    println( new ConcreteTermSubA().newSelf().newSelf().subfoo)
  }
}
