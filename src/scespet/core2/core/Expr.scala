package scespet.core2.core

/**
 * This version is all about trying to get some sort of data-frame going
 */

import scala.reflect.runtime.universe._
import reflect.ClassTag
import stub.gsa.esg.mekon.core.{EventGraphObject, Function => MFunc}
import reflect.api.Exprs

/**
 * @version $Id$
 */


trait FuncCollector{
  def bindSel(src: HasVal[_], sink: Select2[_])

  def bind(src:HasVal[_], sink:Func[_,_])
}


/**
 * Something that provides a value (i.e. a source)
 * @tparam X
 */
trait HasVal[X] {
  def value:X
  def trigger :EventGraphObject
}

/**
 * something that has a source, and is a source (i.e. a pipe)
 * on "calculate", it may take from its source, and update its yielded value
 * if calculate returns false, then state was not modified
 *
 * i.e. this is an operation that can both map and filter
 * @tparam X
 * @tparam Y
 */
trait Func[X,Y] extends HasVal[Y] with MFunc {
  var source:HasVal[X]
  def value:Y
  def trigger = this
  def calculate():Boolean
}


/**
 * this represents a binding of source -> sink
 * it provides operations to build subsequent bindings (e.g. "map")
 * it notifies a FuncCollector about all constructed bindings
 *
 * @param source
 * @param collector
 * @tparam X
 */
class Expr[X](val source:HasVal[X])(implicit collector:FuncCollector) {
//  implicit def selectToFunc[Y <: Select[X]](select:Y) = {new SelectFunc[X,Y](select)}

//  def apply(x: Int): Int = x * 2
  def unapply(): Option[Expr[X]] = {
//    val value: X = this.source.value
    println("unapply gives " + this)
    Some(this)
  }

  def flatMap[Y](f: (X)=>Y): Expr[Y] = {
    val result: Y = f.apply(this.source.value)
    map(f)
  }

  def mapF[Y](f:Func[X,Y]):Expr[Y] = {
    f.source = source
    collector.bind(source, f)
    return new Expr[Y](f)
  }

  def map[Y](f:X => Y):Expr[Y] = {
    mapF(new AnonFunc(f))
  }

  def sel2[Y <: Select2[X]](s:Y):Expr[Y] = {
    s.source = source
    collector.bindSel(source, s)
    return new Expr[Y](new IsVal[Y](s))
  }

  def sel3(s: Select2[X]):Expr[s.type] = {
    s.source = source
    collector.bindSel(source, s)
    return new Expr[s.type](new IsVal[s.type](s))
  }

//  def sel(select:Select[X]):Expr[select.Self] = map(new SelectFunc[X,select.Self](select))
//  def sel[Y <: Select[X]](select:Y):Expr[Y] = {
//    return new Expr[Y](new HasVal[Y] {
//      def value: Y = select
//      def trigger: EventGraphObject = null
//    })
//  }

  def filter(f:X => Boolean):Expr[X] = mapF(new AnonFilterFunc[X](f))

  def sample(n:Long):Expr[X] = mapF(new SampledFunc[X](n))

  def foreach(f:X => Unit) = mapF(new AnonFunc(f))
}



// integrated map and filter. map -> null implies filter
class AnonFunc[X,Y](val f:X => Y) extends AbsFunc[X,Y]{
  def calculate() :Boolean = {
    //    print("Computing "+f+" of "+source.value)
    value=f(source.value)
    return value != null
  }
}

class AnonFilterFunc[X](val f:X => Boolean) extends AbsFunc[X,X]{
  def calculate() = {
    if (f(source.value)) {
      value=source.value
      true
    } else {
      false
    }
  }
}

/**
 * wrap an X as a Func[X,X]
 * @tparam X
 */
class Identity[X]() extends AbsFunc[X,X]{
  def calculate() = {
    value = source.value
    true
  }
}


/**
 * expose every nth sample of X as a Func[X,X]
 * @tparam X
 */
class SampledFunc[X](val n:Long) extends AbsFunc[X,X]{
  var counter = 0L
  def calculate() = {
    counter += 1
    if (counter % n == 0) {
      value = source.value
      true
    } else {
      false
    }
  }
}

/**
 *
 */
class SelectFunc[IN, OUT <: Select[_]](val select:OUT) extends Func[IN,OUT]{
  var source :HasVal[IN] = _
  def value = select

  def calculate() = {
    select.update()
    true
  }
}

class IsVal[X <: EventGraphObject](val value:X) extends HasVal[X] {
  def trigger = value
}


//abstract class Select[IN: TypeTag](implicit tag: TypeTag[IN]) extends DelayedInit {
abstract class Select[IN](implicit tag:ClassTag[IN]) extends DelayedInit {
  //    value = this
  var in:IN = _
//  type Me <: Select[IN]

  var initBody:()=>Unit = _

  override def delayedInit(x: => Unit) {
    initBody = ()=>{x;}
  }

  def update() {
    initBody()
  }

//  def apply():SelectFunc[IN,this.type] = {
//    return new SelectFunc[IN, Select.this.type](this)
//  }



  List(1,2,3).map(x => new {
    lazy val foo = x*2
    lazy val bar = foo * 4
  }).map(x => new {
    lazy val foo2 = x.foo*2
    lazy val bar2 = x.bar*4
  })


  List(1,2,3).map(x => new {
    def foo = x*2
    lazy val bar = foo * 4
  }).map(x => new {
    lazy val foo2 = x.foo*2
    lazy val bar2 = x.bar*4
  })


}



abstract class Select2[IN]() extends DelayedInit with MFunc {
  //    value = this
  var in:IN = _
  var source:HasVal[IN] = _
  //  type Me <: Select[IN]

  var initBody:()=>Unit = _

  override def delayedInit(x: => Unit) {
    initBody = ()=>{x;}
  }

  def calculate():Boolean = {
    in = source.value
    initBody()
    return true
  }

  //  def apply():SelectFunc[IN,this.type] = {
  //    return new SelectFunc[IN, Select.this.type](this)
  //  }
}


