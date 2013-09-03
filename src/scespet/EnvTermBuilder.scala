package scespet

import core._
import core.types
import scespet.expression.{HasValRoot, RootTerm, AbsTerm}
import gsa.esg.mekon.core.EventGraphObject

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 30/04/2013
 * Time: 23:35
 * To change this template use File | Settings | File Templates.
 */
class EnvTermBuilder(val env :types.Env) {

  // I'm sure there's some scala way of making this work without needing a unique method name.
  // if I make this just "query" I get ambiguous method errors.
  def queryE[X](data : HasVal[X]) : MacroTerm[X] = {
    return new MacroTerm[X](env)(data)
  }

  def query[X](data: HasVal[X]) : MacroTerm[X] = {
    return new MacroTerm[X](env)(data)
  }

  def query[X,Y](value: AbsTerm[X, Y]) :Term[Y] = {
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
    query(hasVal)
  }

  def query[X](elements:Iterable[X]) :VectTerm[X,X] = {
    import scala.collection.JavaConverters._
    new VectTerm[X,X](env)(new MutableVector(elements.asJava, env))
  }
}

object EnvTermBuilder {
  implicit def eventObjectToHasVal[X <: EventGraphObject](evtObj:X) :HasVal[X] = new IsVal(evtObj)
}
