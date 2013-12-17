package scespet

import core._
import core.types
import scespet.expression.{HasValRoot, RootTerm, CapturedTerm}
import gsa.esg.mekon.core.EventSource

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 30/04/2013
 * Time: 23:35
 * To change this template use File | Settings | File Templates.
 */
class EnvTermBuilder(val e :types.Env) extends DelayedInit {
  implicit var env = e

  var initBody:()=>Unit = _

  override def delayedInit(x: => Unit) {
    initBody = ()=>{x;}
  }

  def asStream[X](data: HasVal[X]) : MacroTerm[X] = {
    if (data.isInstanceOf[EventSource]) {
      env.registerEventSource(data.asInstanceOf[EventSource])
    }
    return new MacroTerm[X](env)(data)
  }

  def query[X,Y](value: CapturedTerm[X, Y]) :Term[Y] = {
    if (value.parent != null) {
      val localParent = query(value.parent)
      val localThis = value.applyTo(localParent)
      localThis
    } else if (value.isInstanceOf[ RootTerm[Y] ]) {
      var hasVal = value.asInstanceOf[RootTerm[Y]].buildHasVal(env)
      new MacroTerm[Y](env)(hasVal)
    } else {
      ???
    }
  }

  def query[X](newHasVal :(types.Env) => HasVal[X]) : Term[X] = {
    val hasVal = newHasVal(env)
    asStream(hasVal)
  }

  def asVector[X](elements:Iterable[X]) :VectTerm[X,X] = {
    import scala.collection.JavaConverters._
    new VectTerm[X,X](env)(new MutableVector(elements.asJava, env))
  }

//  def reduce[B <: types.MFunc](aggregateBuilder: => B) :ReduceBuilder[B] = {
//    new ReduceBuilder[B](aggregateBuilder)
//  }

}

//class ReduceBuilder[B <: types.MFunc](aggregateBuilder : => B) {
//  val bBuilder = aggregateBuilder
//
//  def join[X](MultiTerm[])
//}

object EnvTermBuilder {
  implicit def eventObjectToHasVal[X <: types.EventGraphObject](evtObj:X) :HasVal[X] = new IsVal(evtObj)
}
